# PyFolderBackup
A Python-based repository designed for backing up folders to Amazon S3 typically involves a script or a set of scripts that automate the process of uploading files from a local directory to an S3 bucket.

## Configuration
Use the template from **__config.tmp.yaml__** to create your own configuration file. The configuration file should be named config.yaml and should be placed in the same directory as the script.
```yaml
backups:
  - name: daily_backup # Name of the backup
    cron: "0 2 * * *" # Cron expression for the backup schedule
    source_dir: /path/to/source # Source directory to back up
    s3:
      bucket: my-bucket
      region: us-west-2
      access_key_id: my-access-key
      secret_access_key: my-secret-key
      sender_email: my-email
      recipient_emails:
        - my-email1
        - my-email1
```

## Running the script
To run the script, simply execute the following command:
```bash
python main.py
```
