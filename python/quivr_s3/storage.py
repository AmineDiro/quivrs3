import asyncio
from math import ceil
import os
from pathlib import Path
from time import time

from typing import Sequence
from uuid import UUID
import logging

from quivr_core.storage.file import QuivrFile, uuid4
from quivr_core.storage.storage_base import StorageBase
from quivr_s3.upload import multipart_upload
import boto3

logger = logging.getLogger("quivrs3")


class QuivRS3(StorageBase):
    name = "QuivRS3"

    def __init__(
        self,
        bucket_name: str = "test",
        s3_endpoint_url: str = "http://127.0.0.1:54321/storage/v1/s3",
        s3_access_key: str = "625729a08b95bf1b7ff351a663f3a23c",
        s3_secret_key: str = "850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907",
        region_name: str = "local",
    ):
        self._client = boto3.client(
            "s3",
            # TODO: Supabase local storage endpoint
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=region_name,
        )
        self.bucket_name = bucket_name
        # 10 MiB
        self.chunk_size = 10 * 1024 * 1024
        self.max_files = 128
        self.parallel_failures = 63
        self.max_retries = 1
        self.n_files = 0

    def nb_files(self) -> int:
        return self.n_files

    async def upload_files(
        self, files: Sequence[QuivrFile], exists_ok: bool = False
    ) -> None:
        await asyncio.gather(
            *[self.upload_file(f) for f in files],
        )
        self.n_files += len(files)

    async def upload_file(self, file: QuivrFile, exists_ok: bool = False) -> None:
        assert file.file_size, "can't multipart upload unknown size file"
        urls = []
        nb_parts = ceil(file.file_size / self.chunk_size)
        upload = self._client.create_multipart_upload(
            ACL="bucket-owner-full-control",
            Bucket=self.bucket_name,
            Key=str(file.id),
        )
        upload_id = upload["UploadId"]
        logger.debug(f"created multipart upload, upload_id={upload_id}")

        for part_number in range(1, nb_parts + 1):
            params = {
                "Bucket": self.bucket_name,
                "Key": str(file.id),
                "PartNumber": part_number,
                "UploadId": upload_id,
            }
            urls.append(
                self._client.generate_presigned_url(
                    ClientMethod="upload_part", Params=params, ExpiresIn=86400
                )
            )
        logger.debug(f"prepared parts urls :{urls}")

        logger.debug("uploading parts...")
        start = time()
        responses = await multipart_upload(
            file_path=file.path.as_posix(),
            parts_urls=urls,
            chunk_size=self.chunk_size,
            max_files=self.max_files,
            parallel_failures=self.parallel_failures,
            max_retries=self.max_retries,
        )
        logger.info(f"uploaded {len(urls)} parts in {time() - start:.3f}s")

        etag_with_parts = []
        for part_number, header in enumerate(responses):
            etag = header["etag"]
            etag_with_parts.append({"ETag": etag, "PartNumber": part_number + 1})

        parts = {"Parts": etag_with_parts}

        self._client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=str(file.id),
            MultipartUpload=parts,
            UploadId=upload_id,
        )
        logger.debug(f"file {file.id} upload complete")

    async def get_files(self) -> list[QuivrFile]:
        raise NotImplementedError("get_file not implemented")

    async def remove_file(self, file_id: UUID) -> None:
        raise NotImplementedError("remove file not implemented")


if __name__ == "__main__":
    import asyncio
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Add the handler to your logger
    quivrs3_logger = logging.getLogger("quivrs3")
    quivrs3_logger.setLevel(logging.DEBUG)
    quivrs3_logger.addHandler(console_handler)
    quivrs3_logger.propagate = False

    async def main():
        logger.info("Starting upload...")
        storage = QuivRS3()
        file_path = Path("./data")
        brain_id = uuid4()
        n_files = 2
        qfiles = [
            QuivrFile(
                id=uuid4(),
                brain_id=brain_id,
                original_filename=file_path.name,
                path=file_path,
                file_size=os.stat(file_path).st_size,
            )
            for _ in range(n_files)
        ]
        qrs3_storage = QuivRS3()
        await qrs3_storage.upload_files(qfiles)

    asyncio.run(main())
