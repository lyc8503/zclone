# zclone
Simple script to backup your ZFS pool to Rclone remote

## Backup

Run `python3 send.py`

## Test your backup / Recover

1. Download your remote backup data to a local directory. You can use `rclone` to do this.
   ```bash
   rclone copy --progress --transfers=16 --multi-thread-streams=16 remote_backup:/MyNAS ./temp/ 
   ```

2. Run the below commands under local directory to recover pool.
   ```bash
   # Enter your password
   read -s PASS
   ls your_pool@your_tag.zst.gpg.part* | sort -V | xargs cat | gpg --batch --passphrase $PASS --decrypt | unzstd | pv | zstream dump
   ```

   To actually recover your data from backup, change the tailing `zstream dump` to `zfs recv testpool`
