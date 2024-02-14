import subprocess
import json
import shlex


REMOTE_PATH = input("Enter a remote/local path (e.g. gdrive:backup/ or /tmp/backup/): ")



def list_files():
    process = subprocess.Popen(["rclone", "lsjson", REMOTE_PATH], stdout=subprocess.PIPE)
    files = json.loads(process.stdout.read().decode())
    return files


def read_block(filename):
    process = subprocess.Popen(["rclone", "cat", "--bind", "0.0.0.0", REMOTE_PATH + filename], stdout=subprocess.PIPE)
    return process.stdout.read()


def get_backups():
    backups = {}
    for f in list_files():
        name = f['Name']
        backup_name, part = name.split(".zst.gpg.part")
        backup_name = backup_name.replace('#', '/')

        if backup_name not in backups:
            backups[backup_name] = []
        backups[backup_name].append((int(part), name))
    return backups


def prompt_backup(backups):
    print("Available backups below:")
    backup_list = list(backups.keys())

    for index, backup in enumerate(backup_list):
        print(f"{index}) {backup.replace('#', '/')}")
    target = backup_list[int(input("\nChoose an index to recover: "))]

    blocks = backups[target]
    blocks.sort(key=lambda x: x[0])
    return target, blocks


target, blocks = prompt_backup(get_backups())
assert list(map(lambda x: x[0], blocks)) == list(range(len(blocks))), "Backup is incomplete"


process = subprocess.Popen(f"gpg -d -v --batch --passphrase {shlex.quote('TODO: KEY')} | zstd -d | zfs recv -v test/test1", shell=True, stdin=subprocess.PIPE)

for i, b in enumerate(blocks):
    print(f"Downloading {b[1]} {i}/{len(blocks)}")
    data = read_block(b[1])

    process.stdin.write(data)
    process.stdin.flush()

process.stdin.close()
