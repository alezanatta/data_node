import boto3
import requests
from time import sleep
from dataclasses import dataclass
from botocore.exceptions import ClientError

from node_exceptions import DataPipelineServerError


@dataclass
class RequestInput:
    url: str
    headers: dict = None
    data: dict = None
    json_data: dict = None
    verify: bool = False
    retry: bool = False
    max_retry: int = 3
    backoff_time: int = 1


@dataclass
class S3Input:
    bucket: str
    file_name: str = None
    obj_data: bytes = None


class NodeRequests:
    def __init__(self, request_input: RequestInput) -> None:
        self.HTTP_STATUS_FORCE_RETRY = [429, 500, 502, 503, 504]
        self.request_input = request_input

    def _request(
        self,
        method: str,
        url: str,
        headers: dict = None,
        json_data: dict = None,
        verify: bool = False,
        retry: bool = False,
        retry_count: int = 0,
        max_retry: int = 3,
        backoff_time: int = 1,
        timeout: int = 10,
    ) -> requests.Response:
        response = None

        while retry_count < max_retry:
            try:
                response = requests.request(
                    method,
                    url,
                    headers=headers,
                    json=json_data,
                    verify=verify,
                    timeout=timeout,
                )
            except requests.exceptions.ConnectionError:
                self.logger.info(
                    f"Connection to: {self.request_input.url} timed out"
                )
                retry_count += 1
                sleep(backoff_time * retry_count)
                self.logger.info(
                    f"Retrying connection to: {self.request_input.url}"
                )
                continue

            if (
                retry
                and response
                and response.status_code in self.HTTP_STATUS_FORCE
            ):
                retry_count += 1
                sleep(backoff_time * retry_count)
                self.logger.info(
                    f"Retrying connection to: {self.request_input.url}"
                )
                continue
            else:
                break
        if retry_count == max_retry:
            raise DataPipelineServerError(
                f"Max number of retries to: {self.request_input.url}"
                " reached.",
                "500" # TODO call error class
            )
        return response

    def post(self) -> requests.Response:
        self.logger.info(f"Making POST request to: {self.request_input.url}")
        response = self._request(
            "POST",
            url=self.request_input.url,
            headers=self.request_input.headers,
            json_data=self.request_input.json_data,
            retry=self.request_input.retry,
            max_retry=self.request_input.max_retry,
            backoff_time=self.request_input.backoff_time,
            timeout=self.request_input.timeout,
        )
        return response

    def get(self) -> requests.Response:
        self.logger.info(f"Making GET request to: {self.request_input.url}")
        response = self._request(
            "GET",
            url=self.request_input.url,
            headers=self.request_input.headers,
            verify=self.request_input.verify,
            retry=self.request_input.retry,
            max_retry=self.request_input.max_retry,
            backoff_time=self.request_input.backoff_time,
        )
        return response


class NodeS3:
    def __init__(self, s3_input: S3Input) -> None:
        self.s3_input = s3_input

    def put_s3_object(self) -> dict:
        try:
            client = boto3.client("s3")
            response = client.put_object(
                Bucket=self.s3_input.bucket,
                Key=self.s3_input.file_name,
                Body=self.s3_input.obj_data,
            )
        except ClientError:
            raise DataPipelineServerError(
                "Error putting object in "
                f"s3://{self.s3_input.bucket}/{self.s3_input.file_name}",
                "500" # TODO call error class
            )
        return response

    def get_s3_object(self) -> dict:
        try:
            client = boto3.client("s3")
            response = client.get_object(
                Bucket=self.s3_input.bucket, Key=self.s3_input.file_name
            )
        except ClientError:
            raise DataPipelineServerError(
                "Error finding object from "
                f"s3://{self.s3_input.bucket}/{self.s3_input.file_name}",
                "500" # TODO call error class
            )
        return response

    def delete_s3_object(self) -> dict:
        try:
            client = boto3.client("s3")
            response = client.delete_object(
                Bucket=self.s3_input.bucket,
                Key=self.s3_input.file_name,
            )
        except ClientError:
            raise DataPipelineServerError(
                "Error deleting object in "
                f"s3://{self.s3_input.bucket}/{self.s3_input.file_name}",
                "500" # TODO call error class
            )
        return response

    def validate_s3(self):
        try:
            client = boto3.client("s3")
            client.list_objects_v2(
                Bucket=self.s3_input.bucket,
                MaxKeys=10
            )
            return None
        except Exception as e:
            return e
