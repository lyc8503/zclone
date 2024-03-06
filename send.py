import string
import subprocess
import shlex
import os
from concurrent.futures import ThreadPoolExecutor, wait
from getpass import getpass
import sys
import traceback


### CONFIG
ENCRYPTION_KEY = getpass("Encryption key: ")
DATASET = os.environ['DATASET']
REMOTE_PATH = os.environ['REMOTE']

PARALLEL_UPLOAD_COUNT = 4

RCLONE_RETRY = 9999


def zfs_full_send_compressed_and_encrypted(pool_name: str, chunk_size=1024*1024*1024, progress=True):
    assert pool_name, "Pool name is required"
    assert isinstance(pool_name, str), "Pool name must be a string"
    assert all([c in (string.ascii_letters + string.digits + "._-@/") for c in pool_name]), "Pool name contains invalid characters"

    if progress:
        process = subprocess.Popen(f"zfs send -nRP {pool_name}", shell=True, stdout=subprocess.PIPE)
        size = 0

        for line in process.stdout.readlines():
            line = line.decode()
            if line.startswith("size"):
                size = int(line.split()[1])
                print(f"Total size: {format(size / 1024 / 1024, '.1f')} MiB")
        pv = f"pv -F '%b %t %r %a %p %e\n' --size {size}"

    command_pipeline = [
        f"zfs send -R {pool_name}",
        *([pv] if progress else []),
        "zstd",
        f"gpg --batch --passphrase {shlex.quote(ENCRYPTION_KEY)} -v --cipher-algo AES256 --s2k-mode 3 --s2k-digest-algo SHA512 --s2k-count 65011712 -z 0 --symmetric"
    ]

    command = " | ".join(command_pipeline)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    while True:
        data = process.stdout.read(chunk_size)
        if not data:
            break
        yield data


def upload_block(data, filename):
    counter = 0
    try:
        while True:
            # Rclone does not write any local files when streaming is supported on the remote end.
            # If the remote does not support streaming, it is necessary to mount tmpfs to /tmp to prevent SSD wear.
            # By default, rclone retries 3 times.
            process = subprocess.Popen(f"./rclone rcat {REMOTE_PATH}{filename}", shell=True, stdin=subprocess.PIPE)
            process.communicate(data)

            if process.returncode != 0:
                counter += 1
                print(f"Failed to upload {filename}, code {process.returncode}, Retry {counter}")
                if counter > RCLONE_RETRY:
                    print("Retry limit reached")
                    sys.exit(-1)
            else:
                print(f"Uploaded {filename}")
                break
    except Exception:
        # Unknown error
        traceback.print_exc()
        sys.exit(-1)


futures = set()

with ThreadPoolExecutor(max_workers=PARALLEL_UPLOAD_COUNT) as executor:
    for index, block in enumerate(zfs_full_send_compressed_and_encrypted(DATASET)):
        filename = DATASET + ".zst.gpg.part" + str(index)
        filename = filename.replace("/", "#")

        # Make sure we don't have too many futures, otherwise we might run out of memory
        # Here we have already read and compressed the next block while we're waiting for the uploads to finish
        if len(futures) >= PARALLEL_UPLOAD_COUNT:
            completed, futures = wait(futures, return_when='FIRST_COMPLETED')

        print(f"Uploading file block {index}, size {len(block)}")
        futures.add(executor.submit(upload_block, block, filename))
        
    wait(futures)


# upload_block(b"", DATASET + "~" + "EOF" + ".zst.aes.part" + str(index + 1))

#TODO: cli args, zfs recv, zfs verify, incremental send, error handling (retry, resume, etc.)
#TODO: parity check?
#TODO: shlex.quote
