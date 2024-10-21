from operator import truediv
from pathlib import Path

import oss2
from oss2.credentials import EnvironmentVariableCredentialsProvider

from omnistore.objstore.objstore import ObjStore


class OSS(ObjStore):
    def __init__(self, endpoint: str, bucket: str):
        # Make sure environments OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET are exist.
        auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
        self._bucket = oss2.Bucket(auth, endpoint, bucket)

    def create_dir(self, dirname: str):
        if not dirname.endswith('/'):
            dirname += '/'
        self._bucket.put_object(dirname, "")

    def delete_dir(self, dirname: str):
        for obj in oss2.ObjectIterator(self._bucket, prefix=dirname):
            self._bucket.delete_object(obj.key)

    def upload(self, src: str, dest: str):
        oss2.resumable_upload(self._bucket, dest, src)

    def upload_dir(self, src_dir: str, dest_dir: str):
        for file in Path(src_dir).rglob("*"):
            if file.is_file():
                dest_path = f"{dest_dir}/{file.relative_to(src_dir)}"
                self.upload(str(file), dest_path)
            elif file.is_dir():
                self.create_dir(f"{dest_dir}/{file.relative_to(src_dir)}/")

    def download(self, src: str, dest: str):
        oss2.resumable_download(self._bucket, src, dest)

    def download_dir(self, src_dir: str, dest_dir: str):
        if not src_dir.endswith("/"):
            src_dir += "/"

        if not dest_dir.endswith("/"):
            dest_dir += "/"

        path = Path(dest_dir)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        for obj in oss2.ObjectIterator(self._bucket, prefix=src_dir, delimiter="/"):
            if obj.is_prefix():  # If this is a folder prefix
                sub_dir = dest_dir + obj.key[len(src_dir):]
                self.download_dir(obj.key, sub_dir)
            else:  # If this is a file
                file_path = Path(dest_dir, obj.key[len(src_dir):])
                if not file_path.parent.exists():
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                self.download(obj.key, str(file_path))

    def delete(self, filename: str):
        return self._bucket.delete_object(filename)

    def exists(self, filename: str):
        return self._bucket.object_exists(filename)