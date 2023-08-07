import datetime
import os
from typing import Any

from src.data_node import NodeConfig
from src.data_node import NodeType
from src.data_node import LambdaExecutor
from src.data_node.commons import (LambdaResponse, StatusCode,
                                   StatusString, HealthCheckResponse,
                                   HealthCheckResource)
from src.data_node.node_exceptions import (DataPipelineClientError,
                                           DataPipelineServerError)


example_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Example Json Schema",
    "description": "Simple example schema",
    "type": "object",
    "required": [
        "validation_field"
    ],
    "properties": {
        "validation_field": {
            "type": "boolean",
            "description": "Added as required to test validation"
        },
        "force_error": {
            "type": "string",
            "description": ("Used to force an especific error. "
                            "For unit testing only"),
            "enum": ["400", "500", "580"]
        },
        "dh_request_method": {
            "type": "string",
            "description": "Used to test RESEND or RETRY logics. TBD",
            "enum": ["RESEND", "RETRY"]
        },
        "request_uri": {
            "type": "string",
            "description": "Used to force Health Check. For unit testing only"
        },
        "test_health_check_error": {
            "type": "boolean",
            "description": ("Used to force Health Check Error. "
                            "For unit testing only")
        },
        "fail_twice": {
            "type": "boolean",
            "description": "Used to test Retry logic"
        }
    }
}


node_config = NodeConfig(
    node_type=NodeType.INGESTION_RECEIVE,
    lambda_bucket=os.environ.get("LAMBDA_BUCKET", ""),
    input_jsonschema_spec=example_json_schema
)


class ThisLambda(LambdaExecutor):
    def identify_source_timestamp(self):
        return datetime.datetime.utcnow()

    def prepare_data(self) -> Any:
        return self.input_data

    def validate_input(self) -> None:
        if self.input_data.get("validation_field", False) is True:
            raise DataPipelineClientError(
                message="Testing Extra Validation Error",
                error_code=1001
            )

    def deliver(self) -> LambdaResponse | None:
        self.logger.info(f"Input Data: {self.input_data}")
        self.logger.info(f"Event: {self.event}")

        if self.input_data.get("force_error") == "580":
            raise DataPipelineServerError(
                message="Forced 580 Error",
                error_code=f"{StatusCode.INTERNAL_ERROR}"
            )
        if self.input_data.get("force_error") == "400":
            raise DataPipelineClientError(
                message="Forced 400 Error",
                error_code=f"{StatusCode.CLIENT_ERROR}"
            )
        if self.input_data.get("fail_twice", False) is True:
            retry_count = int(self.event.get("dh_retry_count", 0))
            if retry_count == 0:
                raise Exception("Fail the First")
            if retry_count == 1:
                raise Exception("Fail the Second")
            if retry_count == 2:
                return
        return LambdaResponse(
            status=StatusString.OK,
            status_code=StatusCode.OK,
            message="Testing Response Message"
        )

    def health_check(self) -> HealthCheckResponse:
        return HealthCheckResponse(
            status=StatusString.UNHEALTHY,
            status_code=StatusCode.INTERNAL_ERROR,
            message="Testing Health Check Message",
            dependecy_checks=[
                HealthCheckResource(
                    resource="Test Resource",
                    status=StatusString.UNHEALTHY,
                    message="Test Resource Error Message"
                ),
                HealthCheckResource(
                    resource="Test Resource 2",
                    status=StatusString.UNHEALTHY,
                    message="Test Resource Error Message 2"
                )
            ]
        )


lambda_handler = ThisLambda(node_config)
