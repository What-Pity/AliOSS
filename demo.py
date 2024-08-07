from AliOSS import OSS
from argparse import ArgumentParser

# Parse command-line arguments
parser = ArgumentParser(
    description='Upload/download files to/from Alibaba Cloud OSS; directories will be compressed into zip files before uploading.')
parser.add_argument('--internal', action='store_true', default=False,
                    help='Use the internal network address of Alibaba Cloud.')
parser.add_argument('--mode', type=str, default='up',
                    help='Upload (upload) / Download (download) mode; default is upload.')
parser.add_argument('--file_path', type=str, help='Local file path.')
parser.add_argument('--file_name', type=str, help='OSS file name.')
parser.add_argument('--target', type=str,
                    help='guangzhou / virginia / beijing / jakarta, more details in `oss_info.py`')
args = parser.parse_args()

target = args.target+"_internal" if args.internal else args.target
oss = OSS.connect_oss(target)

if "down" in args.mode.lower():
    if args.file_name is None:
        raise Exception("In download mode, the filename must be specified.")
    oss.download(args.file_name, args.file_path)
elif "up" in args.mode.lower():
    if args.file_path is None:
        raise Exception("In upload mode, the file path must be specified.")
    oss.upload(args.file_path, args.file_name)
else:
    raise Exception("Error in mode parameter.")
