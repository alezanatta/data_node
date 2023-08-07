import boto3
import pytest

from unittest.mock import MagicMock


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    # Set Lambda Environment Variables to testing values.
    monkeypatch.setenv("LAMBDA_BUCKET", "mock bucket")
    monkeypatch.setenv("TEST_VAR", "MISSING")

    return None


@pytest.fixture(autouse=True)
def mock_boto3(monkeypatch):
    mock_object = MagicMock()
    monkeypatch.setattr(boto3, "client", mock_object)
