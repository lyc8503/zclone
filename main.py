import string
import subprocess
import shlex
import os
from concurrent.futures import ThreadPoolExecutor, wait


### CONFIG
ENCRYPTION_KEY = os.environ['KEY']
DATASET = os.environ['DATASET']
REMOTE_PATH = os.environ['REMOTE']

PARALLEL_UPLOAD_COUNT = 4


def zfs_full_send_compressed_and_encrypted(pool_name: str, chunk_size=1024*1024*1024, progress=True):
    assert pool_name, "Pool name is required"
    assert isinstance(pool_name, str), "Pool name must be a string"
    assert all([c in (string.ascii_letters + string.digits + "._-@/") for c in pool_name]), "Pool name contains invalid characters"

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

    process = subprocess.Popen(f"zfs send -R {pool_name}{pv} | zstd | gpg --batch --passphrase {shlex.quote(ENCRYPTION_KEY)} -v --cipher-algo AES256 --s2k-mode 3 --s2k-digest-algo SHA512 --s2k-count 65600000 -z 0 --symmetric", shell=True, stdout=subprocess.PIPE)

    while True:
        data = process.stdout.read(chunk_size)
        if not data:
            break
        yield data


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
    for index, block in enumerate(zfs_full_send_compressed_and_encrypted(DATASET)):
        filename = DATASET + ".zst.gpg.part" + str(index)
        filename = filename.replace("/", "#")

        # Make sure we don't have too many futures, otherwise we might run out of memory
        # Here we have already read and compressed the next block while we're waiting for the uploads to finish
        if len(futures) >= PARALLEL_UPLOAD_COUNT:
            completed, futures = wait(futures, return_when='FIRST_COMPLETED')

        print(f"Uploading file block {index}")
        futures.add(executor.submit(upload_block, block, filename))
        
    wait(futures)


# upload_block(b"", DATASET + "~" + "EOF" + ".zst.aes.part" + str(index + 1))

#TODO: cli args, zfs recv, zfs verify, incremental send, error handling (retry, resume, etc.)
#TODO: parity check?
