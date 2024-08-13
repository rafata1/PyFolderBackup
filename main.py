import os
import subprocess
from dataclasses import dataclass
from typing import Optional

import boto3
import time

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


@dataclass
class S3Config:
    bucket: str
    access_key: str
    secret_key: str
    region: str
    sender_email: Optional[str]
    recipient_emails: list[str]


@dataclass
class Backup:
    name: str
    cron: str
    source_dir: str
    s3: S3Config


@dataclass
class UploadedFile:
    file_path: str
    size: int
    compress_duration: Optional[int] = None
    upload_duration: Optional[int] = None
    total_duration: Optional[int] = None


class Backuper:
    @staticmethod
    def compress_dir(backup_cnf: Backup) -> str:
        os.makedirs("./tmp", exist_ok=True)
        compressed_file_path = f"./tmp/{backup_cnf.name}_{int(time.time())}.tar.gz"
        compress_command = ["tar", "-czvf", compressed_file_path, backup_cnf.source_dir]
        try:
            subprocess.run(compress_command)
            return compressed_file_path
        except Exception as e:
            print(f"Failed to compress {backup_cnf.source_dir}")
            raise e

    @staticmethod
    def remove_dir(file_path: str):
        remove_command = ["rm", "-rf", file_path]
        try:
            subprocess.run(remove_command)
        except Exception as e:
            print(e)

    @staticmethod
    def upload_to_s3(file_path: str, s3_conf: S3Config):
        print(f"Uploading {file_path} to {s3_conf.bucket}")
        client = boto3.client(
            "s3",
            aws_access_key_id=s3_conf.access_key,
            aws_secret_access_key=s3_conf.secret_key,
            region_name=s3_conf.region
        )
        try:
            file_name = file_path.split("/")[-1]
            client.upload_file(file_path, s3_conf.bucket, file_name)
            print(f"Uploaded {file_path} to {s3_conf.bucket}")
        except Exception as e:
            print(f"Failed to upload to {s3_conf.bucket}")
            print(e)

    @staticmethod
    def send_email(uploaded_file: UploadedFile, backup_conf: Backup):
        s3_conf = backup_conf.s3
        if not s3_conf.sender_email or not s3_conf.recipient_emails:
            return
        print(f"Sending email to {s3_conf.recipient_emails}")
        client = boto3.client(
            "ses",
            aws_access_key_id=s3_conf.access_key,
            aws_secret_access_key=s3_conf.secret_key,
            region_name=s3_conf.region
        )

        to_addresses = s3_conf.recipient_emails
        try:
            client.send_email(
                Source=s3_conf.sender_email,
                Destination={
                    "ToAddresses": to_addresses
                },
                Message={
                    "Subject": {
                        "Data": "Backup uploaded successfully"
                    },
                    "Body": {
                        "Html": {
                            "Data": f"""<p>Backup {backup_conf.name} successfully.</p>
                            <p><strong>File: {uploaded_file.file_path}</strong></p>
                            <p><strong>Size: {uploaded_file.size}<strong></p>
                            <p><strong>Compress Duration: {uploaded_file.compress_duration}s</strong></p>
                            <p><strong>Upload Duration: {uploaded_file.upload_duration}s</strong></p>
                            <p><strong>Total Duration: {uploaded_file.total_duration}s</strong></p>
                            """
                        }
                    }
                }
            )
            print(f"Sent email to {s3_conf.recipient_emails}")
        except Exception as e:
            print(f"Failed to send email to {s3_conf.recipient_emails}")
            print(e)

    def do_backup(self, backup: Backup):
        start_time = time.time()
        compressed_file_path = Backuper.compress_dir(backup)
        compression_duration = int(time.time() - start_time)
        start_time = time.time()
        self.upload_to_s3(compressed_file_path, backup.s3)
        upload_duration = int(time.time() - start_time)
        uploaded_file_size = os.path.getsize(compressed_file_path)
        self.remove_dir(compressed_file_path)
        uploaded_file = UploadedFile(
            file_path=compressed_file_path,
            size=uploaded_file_size,
            compress_duration=compression_duration,
            upload_duration=upload_duration,
            total_duration=int(time.time() - start_time)
        )
        self.send_email(uploaded_file, backup)

    def start(self):
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        scheduler = BlockingScheduler()
        for backup in config["backups"]:
            s3_config = S3Config(**backup['s3'])
            backup_obj = Backup(
                name=backup['name'],
                cron=backup['cron'],
                source_dir=backup['source_dir'],
                s3=s3_config
            )

            cron_parts = backup_obj.cron.split(" ")
            if len(cron_parts) != 5:
                raise ValueError("Invalid cron expression")

            trigger = CronTrigger.from_crontab(backup_obj.cron)
            scheduler.add_job(
                self.do_backup,
                trigger=trigger,
                args=[backup_obj]
            )
            print(f"Added backup job for {backup_obj.name}: ", cron_parts)

            try:
                scheduler.start()
            except (KeyboardInterrupt, SystemExit):
                pass
            scheduler.add_job(self.do_backup, "cron", args=[backup], hour=backup.cron)


backuper = Backuper()
backuper.start()
