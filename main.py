import string
import subprocess
import base64
import os
from concurrent.futures import ThreadPoolExecutor, wait

from Crypto.Cipher import AES


### CONFIG
ENCRYPTION_KEY = os.environ['KEY'].encode()
DATASET = os.environ['DATASET']
REMOTE_PATH = os.environ['REMOTE']

PARALLEL_UPLOAD_COUNT = 2


def zfs_full_send_compressed(pool_name: str, chunk_size=1024*1024*1024, progress=True):
    assert pool_name, "Pool name is required"
    assert isinstance(pool_name, str), "Pool name must be a string"
    assert all([c in (string.ascii_letters + string.digits + "._-@") for c in pool_name]), "Pool name contains invalid characters"

    pv = ""
    if progress:
        process = subprocess.Popen(f"zfs send -nRP {pool_name}", shell=True, stdout=subprocess.PIPE)
        size = 0

        for line in process.stdout.readlines():
            line = line.decode()
            if line.startswith("size"):
                size = int(line.split()[1])
                print(f"Total size: {format(size / 1024 / 1024, '.1f')} MiB")
        pv = f" | pv -F '%b %t %r %a %p %e\n' --size {size}"

    process = subprocess.Popen(f"zfs send -R {pool_name}{pv} | zstd", shell=True, stdout=subprocess.PIPE)

    while True:
        data = process.stdout.read(chunk_size)
        if not data:
            break
        yield data


def hash_encrypt_block(data):
    cipher = AES.new(ENCRYPTION_KEY, AES.MODE_GCM)
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return ciphertext, tag, cipher.nonce


def upload_block(data, filename):
    # TODO: no local cache
    # TODO: custom log level
    process = subprocess.Popen(f"./rclone rcat --log-level ERROR {REMOTE_PATH}{filename}", shell=True, stdin=subprocess.PIPE)
    process.stdin.write(data)
    process.stdin.close()

    ret = process.wait()
    assert ret == 0, f"Failed to upload {filename}, code {ret}"
    print(f"Uploaded {filename}")


futures = set()

with ThreadPoolExecutor(max_workers=PARALLEL_UPLOAD_COUNT) as executor:
    for index, block in enumerate(zfs_full_send_compressed(DATASET)):
        encrypted, tag, nonce = hash_encrypt_block(block)
        del block

        tag, nonce = base64.urlsafe_b64encode(tag).decode(), base64.urlsafe_b64encode(nonce).decode()
        filename = DATASET + "~" + tag + "~" + nonce + ".zst.aes.part" + str(index)

        # Make sure we don't have too many futures, otherwise we might run out of memory
        # Here we have already read and compressed the next block while we're waiting for the uploads to finish
        if len(futures) >= PARALLEL_UPLOAD_COUNT:
            completed, futures = wait(futures, return_when='FIRST_COMPLETED')

        print(f"Uploading file block {index}")
        futures.add(executor.submit(upload_block, encrypted, filename))
        
    wait(futures)


upload_block(b"", DATASET + "~" + "EOF" + ".zst.aes.part" + str(index + 1))

#TODO: cli args, zfs recv, zfs verify, incremental send, error handling (retry, resume, etc.)
