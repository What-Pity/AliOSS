from AliOSS import OSS
from argparse import ArgumentParser

parser = ArgumentParser(description='阿里云OSS上传/下载文件，目录会被压缩成zip文件再上传')
parser.add_argument('--internal', type=bool, default=False, help='是否使用阿里云内网地址')
parser.add_argument('--mode', type=str, default='up',
                    help='上传(upload)/下载(download)模式，默认为upload')
parser.add_argument('--file_path', type=str, help='本地文件路径')
parser.add_argument('--file_name', type=str, help='OSS文件名称')
args = parser.parse_args()

if args.internal:
    oss = OSS.virginia_internal()
else:
    oss = OSS.virginia()

if "down" in args.mode.lower():
    if args.file_name is None:
        raise Exception("下载模式下，必须指定文件名")
    oss.download(args.file_name, args.file_path)
elif "up" in args.mode.lower():
    if args.file_path is None:
        raise Exception("上传模式下，必须指定文件路径")
    oss.upload(args.file_path, args.file_name)
else:
    raise Exception("mode参数错误")
