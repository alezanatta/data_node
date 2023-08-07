import boto3
import datetime
import gzip
import json
import logging
import os
import traceback
import uuid
import jsonschema

from enum import Enum
from pydantic import BaseModel
from pydantic import validate_call
from abc import ABCMeta, abstractmethod
from typing import Any, List, Optional, Union

from datahub_utils import arclambda
from datahub_utils.arclambda import APIErrorEnum, APIResponseError
from datahub_utils.arclambda import build_api_response

from .models import S3Input, NodeS3
from .node_exceptions import DataPipelineClientError


logger = logging.getLogger()

AUDIT_LEVEL = 25

class StatusCode(int, Enum):
    OK = 200
    HEALTHY = 200
    CLIENT_ERROR = 400
    INTERNAL_ERROR = 500
    RETRY_ERROR = 580


class StatusString(str, Enum):
    OK = "OK"
    CLIENT_ERROR = "Client Error"
    INTERNAL_ERROR = "Internal Error"
    RETRY_ERROR = "Error, attempting retries asynchronously"
    UNHEALTHY = "Unhealthy"
    HEALTHY = "Healthy"


class NodeType(str, Enum):
    INGESTION_PULL = "ingestion_pull"
    INGESTION_RECEIVE = "ingestion_receive"
    DOWNSTREAM = "downstream"
    DELIVERY = "delivery"


class RequestMode(str, Enum):
    NORMAL = "normal"
    RESEND = "resend"
    RETRY = "retry"
    HEALTH = "health"


class Format(str, Enum):
    JSON = "json"
    XML = "xml"
    PARQUET = "parquet"


class NodeConfig(BaseModel):
    node_type: NodeType
    lambda_bucket: str
    input_format: str = Format.JSON
    output_format: str = Format.JSON
    s3_subdomain: str = "main"
    dh_retry_attempt_scheme: list = [60, 300, 1800]
    input_jsonschema_spec: Union[str, bytes, dict] = None


class LambdaResponse(BaseModel):
    status: StatusString
    status_code: StatusCode
    message: str
    errors: Optional[List[APIResponseError]] = None


class HealthCheckResource(BaseModel):
    resource: str
    status: StatusString
    message: Optional[str] = None


class HealthCheckResponse(LambdaResponse):
    dependecy_checks: List[HealthCheckResource] = []


INGESTION_TYPES = (NodeType.INGESTION_PULL, NodeType.INGESTION_RECEIVE)


class StateMachine:
    """Encapsulates Step Functions state machine actions."""

    def __init__(self, stepfunctions_client):
        """
        :param stepfunctions_client: A Boto3 Step Functions client.
        """
        self.stepfunctions_client = stepfunctions_client

    def start(self, state_machine_arn, run_input):
        """
        Start a run of a state machine with a specified input. A run is also known
        as an "execution" in Step Functions.

        :param state_machine_arn: The ARN of the state machine to run.
        :param run_input: The input to the state machine, in JSON format.
        :return: The ARN of the run. This can be used to get information about the run,
                 including its current status and final output.
        """
        try:
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn, input=run_input
            )
        except DataPipelineClientError as err:
            # TODO: replace logging
            logger.error(
                "Couldn't start state machine %s. Here's why: %s: %s",
                state_machine_arn,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return response["executionArn"]


class LambdaExecutor(metaclass=ABCMeta):
    @validate_call
    def __init__(self, node_config: NodeConfig) -> None:
        self.config = node_config
        self._setup_log()

    def _setup_log(self, log_level=None) -> None:
        if log_level is None:
            log_level = logging.INFO

        logging.addLevelName(AUDIT_LEVEL, "AUDIT")

        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)

    @arclambda.api
    def __call__(self, event, context):
        self.event = event
        self.context = context
        try:
            response = self._execute()
        except DataPipelineClientError as e:
            response = {
                "status": "Client Error",
                "status_code": 400,
                "errors": [
                    {
                        "source": "DataHub",
                        "code": e.error_code,
                        "message": str(e),
                    }
                ],
            }
        except Exception as e:
            self.logger.error("Internal error executing lambda.")
            tb_str = traceback.format_exc()
            self.logger.error("%s", str(e))
            self.logger.error("Traceback:\n%s", tb_str)
            if self.request_mode == RequestMode.RETRY:
                status_string = StatusString.INTERNAL_ERROR
                status_code = StatusCode.INTERNAL_ERROR
            else:
                self._put_s3_file(self.input_data, self.dh_retry_path)
                self._engage_reprocess_queue()
                status_string = StatusString.RETRY_ERROR
                status_code = StatusCode.RETRY_ERROR

            response = {
                "status": status_string,
                "status_code": status_code,
                "errors": [
                    {
                        "source": "DataHub",
                        "code": APIErrorEnum.internal,
                        "message": str(e),
                    }
                ],
            }
        self.logger.log(AUDIT_LEVEL, response)
        api_response = build_api_response(**response)
        return api_response

    @validate_call
    def _execute(self) -> None:
        self.logger.info("Lambda execution started.")

        self._common_initilization()
        self._validate_config()
        self._identify_request_mode()
        if self.request_mode == RequestMode.HEALTH:
            return self._execute_health_check()

        self._identify_input()

        if self.request_mode == RequestMode.NORMAL:
            response = self._execute_normal()
        elif self.request_mode == RequestMode.RESEND:
            response = self._execute_resend()
        elif self.request_mode == RequestMode.RETRY:
            response = self._execute_retry()

        return response

    def _validate_config(self) -> None:
        if self.config.node_type == NodeType.INGESTION_PULL:
            if not hasattr(self, "get_source_data"):
                raise DataPipelineClientError(
                    "Ingestion Pull Lambda must provide "
                    "get_source_data() function",
                    APIErrorEnum.validation,
                )

        if self.config.node_type in INGESTION_TYPES:
            if not hasattr(self, "identify_source_timestamp"):
                raise DataPipelineClientError(
                    "Ingestion Lambda must provide "
                    "identify_source_timestamp() function",
                    APIErrorEnum.validation,
                )
            if (
                not self.config.input_jsonschema_spec
            ):
                raise DataPipelineClientError(
                    "Ingestion Lambda must provide "
                    "input_jsonschema_spec",
                    APIErrorEnum.validation,
                )

        if self.config.node_type == NodeType.DOWNSTREAM:
            if not hasattr(self, "prepare_data"):
                raise DataPipelineClientError(
                    "Downstream Lambda must have " "prepare_data() function",
                    APIErrorEnum.validation,
                )

    def _identify_request_mode(self) -> None:
        if isinstance(self.event, dict):
            if "health" in self.event.get("request_uri", ""):
                self.request_mode = RequestMode.HEALTH
            elif "dh_request_mode" in self.event:
                self.request_mode = RequestMode(self.event["dh_request_mode"])
            else:
                self.request_mode = RequestMode.NORMAL
        else:
            self.request_mode = RequestMode.NORMAL
        self.logger.info("dh_request_mode = %s", self.request_mode)

    def _common_initilization(self) -> None:

        self.function_name = self.context.function_name
        try:
            with open("/VERSION", "r") as fp:
                self.lambda_image_version = fp.read().strip()
        except Exception:
            self.lambda_image_version = "unknown"
        self.logger.info(
            "Lambda: %s Image Version: %s",
            self.function_name,
            self.lambda_image_version,
        )

    def _identify_input(self) -> None:
        """Function responsable for identifing the requester payload and
        setting it into an input_data attribute.
        It only works if executed after _identify_request_mode"""

        if self.request_mode == RequestMode.NORMAL:
            if self.config.node_type in INGESTION_TYPES:
                if (
                    isinstance(self.event, dict)
                    and "request_body" in self.event
                ):
                    self.input_data = self.event["request_body"]
                else:
                    self.input_data = self.event
            elif self.config.node_type == NodeType.DOWNSTREAM:
                if isinstance(self.event, dict):
                    try:
                        self.input_data = self.event["dh_data"]
                        self.dh_pipeline_key = self.event["dh_pipeline_key"]
                    except KeyError as e:
                        raise DataPipelineClientError(
                            (f"Error getting {str(e)} key from event. "
                             "DOWNSTREAM Lambdas expect dh_data and dh_pipeline_key"),
                            APIErrorEnum.data_type
                        )
                else:
                    raise DataPipelineClientError(
                        "Downstream lambda payload should be a dict",
                        APIErrorEnum.data_type,
                    )

        # Both RESEND and RETRY acquire data from S3, so no input identification
        elif self.request_mode == RequestMode.RESEND:
            pass
        elif self.request_mode == RequestMode.RETRY:
            pass

    def _validate_input(self) -> None:
        """Executes input validation
        If a JsonSchema is provided, validates the data against it
        using jsonschema module.
        Finally, executes additional validation provided by the developer
        in the validate_input method.
        This must be executed after _identify_request_mode and _identify_input"""

        if self.config.input_jsonschema_spec is not None:
            if isinstance(self.config.input_jsonschema_spec, dict):
                input_schema = self.config.input_jsonschema_spec
            else:
                input_schema = json.loads(self.config.input_jsonschema_spec)

        logger.info("validate_input()")
        error_msg = ""
        input_validator = jsonschema.Draft7Validator(
            input_schema, format_checker=jsonschema.draft7_format_checker
        )
        for error in sorted(input_validator.iter_errors(self.input_data), key=str):
            error_path = ""
            if error.path:
                error_path = f"payload[{']['.join(repr(i) for i in error.path)}]"
            else:
                error_path = "root of payload"
            if error.validator == "type":
                error_msg = (
                    f"Invalid type, {error_path} is not of "
                    f"type {error.validator_value}"
                )
            elif error.validator == "required":
                error_msg = f"{error.message} on {error_path}"
            else:
                error_msg = (
                    f"Failed validating {error.validator} on {error_path}; "
                    f"{error.message}"
                )
        if error_msg != "":
            raise DataPipelineClientError(
                error_msg,
                APIErrorEnum.validation
            )

        self.validate_input()

    def _get_source_timestamp(self) -> None:
        self.source_timestamp = self.identify_source_timestamp()
        if not isinstance(self.source_timestamp, datetime.datetime):
            raise Exception(
                "identify_source_timestamp() should return a datetime object"
            )

    def _generate_pipeline_key(self) -> None:
        pipeline_uuid = uuid.uuid4().hex
        self.dh_pipeline_key = (
            f"{self.config.s3_subdomain}/"
            f"{self.source_timestamp:%Y}/"
            f"{self.source_timestamp:%m}/"
            f"{self.source_timestamp:%d}/"
            f"{pipeline_uuid}"
        )
        self.output_path = (
            f"{self.dh_pipeline_key}.{self.config.output_format}.gz"
        )
        self.dh_retry_path = (
            "dlq/"
            f"{self.source_timestamp:%Y}/"
            f"{self.source_timestamp:%m}/"
            f"{self.source_timestamp:%d}/"
            f"{pipeline_uuid}.{self.config.input_format}.gz"
        )
        self.logger.info(f"dh_pipeline_key={self.dh_pipeline_key}")

    def _get_pipeline_key(self) -> None:
        self.dh_pipeline_key = self.event["dh_pipeline_key"]
        self.output_path = (
            f"{self.dh_pipeline_key}.{self.config.output_format}.gz"
        )
        if "dh_retry_path" in self.event:
            self.dh_retry_path = self.event["dh_retry_path"]
            self.dh_pipeline_key = self.event["dh_pipeline_key"]
        else:
            self.dh_retry_path = (
                self.dh_pipeline_key.replace(
                    self.config.s3_subdomain, "dlq", 1
                )
                + ".gz"
            )

    def _execute_normal(self) -> None:

        self.logger.info("Executing Lambda in NORMAL request mode.")
        if self.config.node_type in INGESTION_TYPES:
            self._validate_input()
            self._get_source_timestamp()
            self._generate_pipeline_key()
        elif self.config.node_type == NodeType.DOWNSTREAM:
            self._get_pipeline_key()

        self.output_data = self.prepare_data()
        self._put_s3_file(self.output_data, self.output_path)
        lambda_response = self.deliver()
        response = {
            **lambda_response.model_dump(exclude_none=True),
            "dh_pipeline_key": self.dh_pipeline_key,
        }
        self.logger.info("Executed Lambda in NORMAL request mode.")
        return response

    def _execute_resend(self) -> None:

        self.logger.info("Executing Lambda in RESEND request mode.")
        self._get_pipeline_key()
        self.output_data = self._get_s3_file(self.output_path)
        lambda_response = self.deliver()
        response = {
            **lambda_response.model_dump(exclude_none=True),
            "dh_pipeline_key": self.dh_pipeline_key,
        }
        self.logger.info("Executed Lambda in RESEND request mode.")
        return response

    def _execute_retry(self) -> None:

        self.logger.info("Executing Lambda in RETRY request mode.")
        self._get_pipeline_key()
        self.input_data = self._get_s3_file(self.dh_retry_path)
        self.output_data = self.prepare_data()
        self._put_s3_file(self.output_data, self.output_path)
        lambda_response = self.deliver()
        self.logger.info("Executed Lambda in RETRY request mode.")
        # Retry Executed Successfully, remove the S3 DLQ file.
        try:
            self._delete_s3_file(self.dh_retry_path)
        except Exception as e:
            self.logger.warn(
                "Failed to remove retry file %s. Error: %s",
                self.dh_retry_path,
                str(e),
            )
        response = {
            **lambda_response.model_dump(exclude_none=True),
            "dh_pipeline_key": self.dh_pipeline_key,
        }
        return response

    def _execute_health_check(self) -> None:

        self.logger.info("Executing Lambda in HEALTH CHECK request mode.")
        health_check_response = self._health_check()
        response = {
            **health_check_response.model_dump(exclude_none=True)
        }
        self.logger.info("Executed Lambda in HEALTH CHECK request mode.")
        return response

    def _put_s3_file(self, data, obj_name) -> None:

        if self.config.output_format == Format.JSON and isinstance(data, dict):
            data = json.dumps(data)
        data = data.encode("UTF-8")
        data = gzip.compress(data)

        s3_input = S3Input(
            bucket=self.config.lambda_bucket,
            file_name=obj_name,
            obj_data=data,
        )
        self.logger.info(
            "Put s3://%s/%s", self.config.lambda_bucket, obj_name
        )
        NodeS3(s3_input=s3_input).put_s3_object()

    def _get_s3_file(self, object_name) -> dict:

        s3_input = S3Input(
            bucket=self.config.lambda_bucket,
            file_name=object_name,
        )
        self.logger.info(
            "Get s3://%s/%s", self.config.lambda_bucket, object_name
        )
        s3data = NodeS3(s3_input=s3_input).get_s3_object()
        data = s3data["Body"].read()
        data = gzip.decompress(data)
        try:
            data = json.loads(data)
        except Exception:
            # TODO
            pass

        return data

    def _delete_s3_file(self, object_name) -> None:

        s3_input = S3Input(
            bucket=self.config.lambda_bucket,
            file_name=object_name,
        )
        self.logger.info(
            "Delete s3://%s/%s", self.config.lambda_bucket, object_name
        )
        NodeS3(s3_input=s3_input).delete_s3_object()

    def _engage_reprocess_queue(self) -> None:
        run_input = json.dumps(
            {
                "dh_retry_lambda": self.function_name,
                "dh_pipeline_key": self.dh_pipeline_key,
                "dh_retry_path": self.dh_retry_path,
                "dh_retry_attempt_scheme": self.config.dh_retry_attempt_scheme,
            }
        )
        self.logger.info("boto3 get account_id")
        os.environ["AWS_STS_REGIONAL_ENDPOINTS"] = "regional"
        sts_client = boto3.client("sts")
        account_id = sts_client.get_caller_identity().get("Account")

        state_machine_arn = (
            f"arn:aws:states:us-east-1:{account_id}:" # TODO var for region
            "stateMachine:lambda_reprocess_queue"
        )
        self.logger.info("boto3 state_machine.start()")
        state_machine = StateMachine(boto3.client("stepfunctions"))
        state_machine.start(state_machine_arn, run_input)

    def _health_check(self) -> HealthCheckResponse:

        check = self.health_check()

        s3_input = S3Input(
            bucket=self.config.lambda_bucket
        )
        s3_check = NodeS3(s3_input=s3_input).validate_s3()
        s3_resource = None
        if s3_check:
            s3_resource = HealthCheckResource(
                resource="S3",
                status=StatusString.UNHEALTHY,
                message=str(s3_check)
            )
            check.status = StatusString.UNHEALTHY
            check.status_code = StatusCode.INTERNAL_ERROR
        else:
            s3_resource = HealthCheckResource(
                resource="S3",
                status=StatusString.HEALTHY
            )
        check.dependecy_checks.append(s3_resource)

        return check

    @abstractmethod
    @validate_call
    def deliver(self) -> LambdaResponse:
        """Implement the data deliver here. This should return a LambdaResponse object.
        It will be used to build the API Response."""
        pass

    @abstractmethod
    @validate_call
    def health_check(self) -> HealthCheckResponse:
        """Implement this method to perform health checks. You do not need to
        perform S3 validation for the main lambda bucket"""
        pass

    @abstractmethod
    def validate_input(self) -> None:
        """Implement this method for aditional validation. Json Schemas
        are handled by the framework, if provided"""
        pass

    @abstractmethod
    def prepare_data(self) -> Any:
        """Your business logic should be here. This function is expected to return
        the data to be inserted into S3 as bytes. For ingestion lambdas or lambdas
        without data transformation just call the super method"""
        return self.input_data
