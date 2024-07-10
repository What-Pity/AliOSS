import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider
from oss2 import SizedFileAdapter, determine_part_size
from oss2.models import PartInfo

from pathlib import Path
import logging
import shutil
from typing import Union

# from tqdm import tqdm
import sys


def _create_logger():
    logging.basicConfig(level=logging.WARNING,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.INFO)
    # 创建一个文件处理器
    file_handler = logging.FileHandler('OSS.log')
    # 设置处理器的格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    # 将处理器添加到记录器
    logger.addHandler(file_handler)
    return logger


class OSS:

    logger = _create_logger()

    def __init__(self, end_point: str, bucket_name: str, key_id: Union[str, None] = None, key_secret: Union[str, None] = None, acc: bool = False) -> None:
        if key_id is None or key_secret is None:
            # 从环境变量中获取访问凭证。运行本代码示例之前，请确保已设置环境变量OSS_ACCESS_KEY_ID和OSS_ACCESS_KEY_SECRET。
            self.auth = oss2.ProviderAuth(
                EnvironmentVariableCredentialsProvider())
        else:
            self.auth = oss2.Auth(key_id, key_secret)
        # 填写Bucket所在地域对应的Endpoint。以华东1（杭州）为例，Endpoint填写为https://oss-cn-hangzhou.aliyuncs.com。
        self.service = oss2.Service(self.auth, end_point)
        self.bucket = oss2.Bucket(self.auth, end_point, bucket_name)
        if acc:
            self.bucket.put_bucket_transfer_acceleration("true")
            self.logger.info("使用传输加速")

    @classmethod
    def virginia(cls):
        return cls('https://oss-us-east-1.aliyuncs.com', "oversea-download", acc=True)

    @classmethod
    def virginia_internal(cls):
        return cls('https://oss-us-east-1-internal.aliyuncs.com', "oversea-download", acc=True)

    def _list_buckets(self):
        # 列举当前账号所有地域下的存储空间。
        for b in oss2.BucketIterator(self.service):
            print(b.name)

    def _upload_normal(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # 简单上传
        if not file_path.exists():
            self.logger.error(f"{file_path} 路径不存在")
            return
        if not file_name:
            file_name = file_path.name
        self.bucket.put_object_from_file(
            file_name, file_path, progress_callback=percentage)
        self.logger.info(f"已将 {file_path} 上传至 {file_name}")

    def _upload_multipart(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # 分片上传
        file_stats = file_path.stat()
        total_size = file_stats.st_size
        if file_name is None:
            file_name = file_path.name
        # determine_part_size方法用于确定分片大小。
        part_size = determine_part_size(total_size, preferred_size=100 * 1024)
        upload_id = self.bucket.init_multipart_upload(file_name).upload_id

        parts = []
        # 逐个上传分片。
        with open(file_path, 'rb') as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                # 调用SizedFileAdapter(fileobj, size)方法会生成一个新的文件对象，重新计算起始追加位置。
                result = self.bucket.upload_part(file_name, upload_id, part_number,
                                                 SizedFileAdapter(fileobj, num_to_upload), progress_callback=percentage)
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1

        # 完成分片上传。
        self.bucket.complete_multipart_upload(file_name, upload_id, parts)
        self.logger.info(f"已将 {file_path} 上传至 {file_name}")

    def _upload_resumable(self, file_path: Path, file_name: Union[str, None] = None) -> None:
        # 断点续传
        if file_name is None:
            file_name = file_path.name
        oss2.resumable_upload(self.bucket, file_path,
                              file_name, progress_callback=percentage)

    def upload(self, file_path: str, file_name: Union[str, None] = None, resumalble: bool = False) -> None:
        file_path = Path(file_path)
        if not file_path.exists():
            self.logger.error(f"{file_path} 不存在")
            return
        elif file_path.is_dir():
            self.logger.warning(f"{file_path} 是一个目录，自动压缩为zip文件")
            shutil.make_archive(str(file_path), "zip", str(file_path))
            file_path = Path(str(file_path)+".zip")
        file_stats = file_path.stat()
        total_size = file_stats.st_size
        if total_size/5/1024 > 1024*1024:
            self.logger.info(f"{file_path} 大于5GB，使用分片上传")
            # 大于5GB，使用分片上传
            if resumalble:
                self._upload_resumable(file_path, file_name)
            else:
                self._upload_multipart(file_path, file_name)
        else:
            self._upload_normal(file_path, file_name)

    def _list_objects(self) -> None:
        # 列举当前Bucket下的所有文件。
        for b in oss2.ObjectIteratorV2(self.bucket):
            print(b.key)

    def _download_normal(self, file_name: str, file_path: Union[str, None] = None) -> None:
        # 简单下载
        if file_path is None:
            file_path = './'+file_name
        self.bucket.get_object_to_file(
            file_name, file_path, progress_callback=percentage)
        print("\n")
        self.logger.info(f"已将 {file_name} 下载至 {file_path}")

    def _download_multipart(self, file_name: str, file_path: Union[str, None] = None) -> None:
        # 分片下载
        if file_path is None:
            file_path = './'+file_name
        oss2.resumable_download(self.bucket, file_name, file_path,
                                progress_callback=percentage)
        print("\n")
        self.logger.info(f"已将 {file_name} 下载至 {file_path}")

    def download(self, file_name: str, file_path: Union[str, None] = None) -> None:
        if not self.bucket.object_exists(file_name):
            self.logger.error(f"文件{file_name} 不存在")
            print("可供下载的文件列表：")
            self._list_objects()
            return
        obj_info = self.bucket.head_object(file_name)
        total_size = obj_info.content_length/1024/1024/1024
        if total_size > 5:
            # 大于5GB，使用分片下载
            self.logger.info(f"{file_name} 大于5GB，使用分片下载")
            self._download_multipart(file_name, file_path)
        else:
            self.logger.info(f"使用简单下载")
            self._download_normal(file_name, file_path)


def percentage(consumed_bytes, total_bytes):
    if total_bytes:
        rate = int(100 * (float(consumed_bytes) / float(total_bytes)))
        print('\r'+int(rate)*'='+'>'+'{0}% '.format(rate), end='')
        sys.stdout.flush()


# def tqdm_percentage(consumed_bytes, total_bytes):
#     if total_bytes:
#         pbar = tqdm(total=total_bytes)
#         pbar.update(consumed_bytes)
#         pbar.close()
