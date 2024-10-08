import pprint
import boto3
import os
from pathlib import Path
import pytest
from uuid import uuid4
from quivr_core.files.file import QuivrFile
from quivr_s3.storage import QuivRS3
import logging


def pytest_configure(config):
    # Disable all loggers
    logging.disable(logging.CRITICAL)

    # Re-enable and set level for quivrs3 logger
    quivrs3_logger = logging.getLogger("quivrs3")
    quivrs3_logger.disabled = False
    quivrs3_logger.setLevel(logging.DEBUG)


@pytest.fixture
def qfile() -> QuivrFile:
    file_path = Path(
        "/Users/aminedirhoussi/Documents/coding/quivr-s3/python/tests/python.pdf"
    )
    assert file_path.exists()
    file_size = os.stat(file_path).st_size
    file_id = uuid4()
    return QuivrFile(
        id=file_id,
        brain_id=uuid4(),
        file_extension=".pdf",
        file_size=file_size,
        original_filename=f"python_{file_id}.pdf",
        path=file_path,
        file_md5="md5",
    )


@pytest.mark.asyncio
async def test_multiple_sync(qfile: QuivrFile):
    N = 1
    qstorage = QuivRS3()
    qfiles = [qfile for _ in range(N)]
    await qstorage.upload_files(qfiles)
    assert qstorage.n_files == N


@pytest.mark.skip
def test_boto3_ls():
    s3_endpoint_url: str = "http://127.0.0.1:54321/storage/v1/s3"
    s3_access_key: str = "625729a08b95bf1b7ff351a663f3a23c"
    s3_secret_key: str = (
        "850181e4652dd023b7a98c58ae0d2d34bd487ee0cc3254aed6eda37307425907"
    )
    region_name: str = "local"
    client = boto3.client(
        "s3",
        # TODO: Supabase local storage endpoint
        endpoint_url=s3_endpoint_url,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        region_name=region_name,
    )
    response = client.list_objects(Bucket="test")
    pprint.pprint(response)
