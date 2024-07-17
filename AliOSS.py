import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo

from pathlib import Path
import logging
import shutil
from typing import Union, Optional

# from tqdm import tqdm
import sys


def _create_logger():
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('Ali OSS logger')
    logger.setLevel(logging.INFO)
    # Create a file handler
    file_handler = logging.FileHandler('OSS.log')
    # Set the formatter for the handler
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # Add the handler to the logger
    logger.addHandler(file_handler)
    return logger


class OSS:

    logger = _create_logger()

    def __init__(self, end_point: str, bucket_name: str, key_id: Union[str, None] = None, key_secret: Union[str, None] = None, acc: bool = False) -> None:
        if key_id is None or key_secret is None:
            # Get access credentials from environment variables. Before running this code example, ensure that the environment variables OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET are set.
            self.auth = oss2.ProviderAuth(
                EnvironmentVariableCredentialsProvider())
        else:
            self.auth = oss2.Auth(key_id, key_secret)
        # Fill in the Endpoint corresponding to the region where the Bucket is located. For example, for East China 1 (Hangzhou), the Endpoint is https://oss-cn-hangzhou.aliyuncs.com.
        self.service = oss2.Service(self.auth, end_point)
        self.bucket = oss2.Bucket(self.auth, end_point, bucket_name)
        if acc:
            self.bucket.put_bucket_transfer_acceleration("true")
            self.logger.info("Using transfer acceleration")

    @classmethod
    def connect_oss(cls, mode: str) -> Optional["OSS"]:
        import json
        with open("oss_info.json", "r") as f:
            oss_info = json.load(f)
            for info in oss_info:
                if info["name"] == mode:
                    cls.logger.info(f"Connected to {info['name']}")
                    return cls(end_point=info["end_point"], bucket_name=info["bucket_name"], acc=info["transfer_acceleration"])
            raise ValueError("Mode not found")

    def _list_buckets(self):
        # List all Buckets under the current account in all regions.
        for b in oss2.BucketIterator(self.service):
            print(b.name)

    def _upload_normal(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # Simple upload
        if not file_path.exists():
            self.logger.error(f"{file_path} path does not exist")
            return
        if not file_name:
            file_name = file_path.name
        self.bucket.put_object_from_file(
            file_name, file_path, progress_callback=percentage)
        self.logger.info(f"Uploaded {file_path} to {file_name}")

    def _upload_multipart(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # Multipart upload
        file_stats = file_path.stat()
        total_size = file_stats.st_size
        if file_name is None:
            file_name = file_path.name
        # The determine_part_size method is used to determine the chunk size.
        part_size = determine_part_size(
            total_size, preferred_size=1024 * 1024)
        upload_id = self.bucket.init_multipart_upload(file_name).upload_id

        parts = []
        # Upload chunks one by one.
        with open(file_path, 'rb') as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                percentage(offset, total_size)
                num_to_upload = min(part_size, total_size - offset)
                # Calling SizedFileAdapter(fileobj, size) generates a new file object, recalculating the starting append position.
                result = self.bucket.upload_part(
                    file_name, upload_id, part_number, SizedFileAdapter(fileobj, num_to_upload))
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1

        # Complete multipart upload.
        self.bucket.complete_multipart_upload(file_name, upload_id, parts)
        self.logger.info(f"Uploaded {file_path} to {file_name}")

    def _upload_resumable(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # Resumeable upload
        if file_name is None:
            file_name = file_path.name
        oss2.resumable_upload(self.bucket, file_path,
                              file_name, progress_callback=percentage)

    def upload(self, file_path: str, file_name: Union[str, None] = None, resumable: bool = False) -> None:
        file_path = Path(file_path)
        if not file_path.exists():
            self.logger.error(f"{file_path} does not exist")
            return
        elif file_path.is_dir():
            self.logger.warning(
                f"{file_path} is a directory, automatically compressing to zip file")
            shutil.make_archive(str(file_path), "zip", str(file_path))
            self.logger.info(f"Compressing {file_path} to {file_path}.zip")
            file_path = Path(str(file_path)+".zip")
        file_stats = file_path.stat()
        total_size = file_stats.st_size
        if total_size/5/1024 > 1024*1024:
            self.logger.info(
                f"{file_path} is larger than 5GB, using multipart upload")
            # Larger than 5GB, use multipart upload
            if resumable:
                self._upload_resumable(file_path, file_name)
            else:
                self._upload_multipart(file_path, file_name)
        else:
            self._upload_normal(file_path, file_name)

    def _list_objects(self) -> None:
        # List all objects in the current Bucket.
        for b in oss2.ObjectIteratorV2(self.bucket):
            print(b.key)

    def _download_normal(self, file_name: str, file_path: Union[str, None] = None) -> None:
        # Simple download
        if file_path is None:
            file_path = './'+file_name
        self.bucket.get_object_to_file(
            file_name, file_path, progress_callback=percentage)
        # print("\n")
        self.logger.info(f"Downloaded {file_name} to {file_path}")

    def _download_multipart(self, file_name: str, file_path: Union[str, None] = None) -> None:
        # Multipart download
        if file_path is None:
            file_path = './'+file_name
        oss2.defaults.connection_pool_size = 8
        oss2.resumable_download(self.bucket, file_name, file_path,
                                progress_callback=percentage, store=oss2.ResumableDownloadStore(root='./'), part_size=1024*1024, num_threads=8)
        self.logger.info(f"Downloaded {file_name} to {file_path}")

    def download(self, file_name: str, file_path: Union[str, None] = None) -> None:
        if not self.bucket.object_exists(file_name):
            self.logger.error(f"The file {file_name} does not exist")
            print("List of available files for download:")
            self._list_objects()
            return
        obj_info = self.bucket.head_object(file_name)
        total_size = obj_info.content_length/1024/1024/1024
        if total_size > 5:
            # Larger than 5GB, use multipart download
            self.logger.info(
                f"{file_name} is larger than 5GB, using multipart download")
            self._download_multipart(file_name, file_path)
        else:
            self.logger.info(f"Using simple download")
            self._download_normal(file_name, file_path)


def percentage(consumed_bytes, total_bytes):
    if total_bytes:
        rate = 100 * (float(consumed_bytes) / float(total_bytes))
        consumed = approparate_byte(consumed_bytes)
        total = approparate_byte(total_bytes)
        print('\r'+int(rate)*'='+'>' +
              '{0:.2f}%'.format(rate)+4*' '+'['+consumed+'/'+total+']', end='')
        sys.stdout.flush()
        if rate == 100:
            print('\n')


def approparate_byte(byte):
    if byte < 1024:
        return '{0:.2f}B'.format(byte)
    elif byte < 1024*1024:
        return '{0:.2f}KB'.format(byte/1024)
    elif byte < 1024*1024*1024:
        return '{0:.2f}MB'.format(byte/1024/1024)
    else:
        return '{0:.2f}GB'.format(byte/1024/1024/1024)
# def tqdm_percentage(consumed_bytes, total_bytes):
#     if total_bytes:
#         pbar = tqdm(total=total_bytes)
#         pbar.update(consumed_bytes)
#         pbar.close()
