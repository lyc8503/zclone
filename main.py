import string
import subprocess
import base64

from Crypto.Cipher import AES


### CONFIG
ENCRYPTION_KEY = "E5eb&xS$fbcW5Kx$".encode()
DATASET = "test@2"
REMOTE_PATH = "njuod:/HOMELAB_ZFS_20240209/"


def zfs_full_send_compressed(pool_name: str, chunk_size=2*1024*1024*1024):
    assert pool_name, "Pool name is required"
    assert isinstance(pool_name, str), "Pool name must be a string"
    assert all([c in (string.ascii_letters + string.digits + "._-@") for c in pool_name]), "Pool name contains invalid characters"

    process = subprocess.Popen(f"zfs send {pool_name} | zstd", shell=True, stdout=subprocess.PIPE)

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
    process = subprocess.Popen(f"./rclone rcat {REMOTE_PATH}{filename}", shell=True, stdin=subprocess.PIPE)
    process.stdin.write(data)
    process.stdin.close()

    ret = process.wait()
    assert ret == 0, f"Failed to upload {filename}, code {ret}"


for index, block in enumerate(zfs_full_send_compressed(DATASET)):
    print(f"Uploading block {index}")

    encrypted, tag, nonce = hash_encrypt_block(block)
    del block

    tag, nonce = base64.urlsafe_b64encode(tag).decode(), base64.urlsafe_b64encode(nonce).decode()
    try:
        upload_block(encrypted, DATASET + "~" + tag + "~" + nonce + ".zst.aes.part" + str(index))
    except Exception as e:
        print(f"Failed to upload {DATASET}~{tag}~{nonce}.zst.aes.part{index}: {e}")


upload_block(b"", DATASET + "~" + "EOF" + ".zst.aes.part" + str(index + 1))
