# import boto3

# from tests.conftest import FIXTURES_PATH


# def setup_s3_bucket(with_content: bool | None = False):
#     s3 = boto3.resource("s3", region_name="us-east-1")
#     s3.create_bucket(Bucket="runpandarun")
#     if with_content:
#         client = boto3.client("s3")
#         for f in ("testdata.csv", "testdata.json", "rki.json"):
#             client.upload_file(FIXTURES_PATH / f, "runpandarun", f)
