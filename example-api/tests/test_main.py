import json
import main


class mock_context:
    function_name = "example-api"


def test_lambda_handler():

    event = {
        "validation_field": False
    }
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 200
    assert response_body["message"] == "Testing Response Message"
    assert response_body["status"] == "OK"
    assert "dh_pipeline_key" in response_body


def test_forced_580_error():

    event = {
        "validation_field": False,
        "force_error": "580"
    }
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 580
    assert (
        response_body["status"] == "Error, attempting retries asynchronously"
    )
    assert response_body["errors"] == [
        {"code": "9000", "message": "Forced 580 Error", "source": "DataHub"}
    ]


def test_forced_400_error():

    event = {
        "validation_field": False,
        "force_error": "400"
    }
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert response_body["status"] == "Client Error"
    assert response_body["errors"] == [
        {"code": "400", "message": "Forced 400 Error", "source": "DataHub"}
    ]


def test_validation_error():

    event = {}
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert (
        response_body["status"] == "Client Error"
    )
    assert response_body["errors"] == [
        {
            "code": "1000",
            "message": "'validation_field' is a required property on root of payload",
            "source": "DataHub"
        }
    ]


def test_extra_validation_error():

    event = {
        "validation_field": True,
    }
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 400
    assert (
        response_body["status"] == "Client Error"
    )
    assert response_body["errors"] == [
        {
            "code": "1000",
            "message": "Testing Extra Validation Error",
            "source": "DataHub"
        }
    ]


def test_health_check():

    event = {
        "validation_field": False,
        "request_uri": "health"
    }
    response = main.lambda_handler(event, mock_context)

    response_body = json.loads(response["body"])
    assert response["statusCode"] == 500
    assert (
        response_body["status"] == "Unhealthy"
    )
    assert response_body["message"] == "Testing Health Check Message"
    assert response_body["dependecy_checks"] == [
        {
            "resource": "Test Resource",
            "status": "Unhealthy",
            "message": "Test Resource Error Message"
        },
        {
            "resource": "Test Resource 2",
            "status": "Unhealthy",
            "message": "Test Resource Error Message 2"
        },
        {
            "resource": "S3",
            "status": "Healthy"
        }
    ]
