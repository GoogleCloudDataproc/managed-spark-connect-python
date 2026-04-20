# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import aiplatform_v1
from google.cloud.aiplatform_v1.types import JobState

import os
import uuid
import pytest
import logging

LOGGER = logging.getLogger(__name__)

REPOSITORY_ID = "97193e1e-c5d1-4ce8-bc6f-cf206c701624"
TEMPLATE_ID = "6409629422399258624"


@pytest.fixture
def test_project():
    return os.getenv("GOOGLE_CLOUD_PROJECT")


@pytest.fixture
def test_region():
    return os.getenv("GOOGLE_CLOUD_REGION")


@pytest.fixture
def test_service_account():
    return os.getenv("DATAPROC_SPARK_CONNECT_SERVICE_ACCOUNT")


@pytest.fixture
def test_template():
    return TEMPLATE_ID


@pytest.fixture
def test_repository():
    return REPOSITORY_ID


def test_executing_colab_notebook(
    test_project,
    test_region,
    test_service_account,
    test_template,
    test_repository,
):
    """Test executing a Colab notebook that uses Spark Connect."""
    test_api_endpoint = f"{test_region}-aiplatform.googleapis.com"
    test_parent = f"projects/{test_project}/locations/{test_region}"
    test_execution_display_name = (
        f"spark-connect-e2e-notebook-test-{uuid.uuid4().hex}"
    )

    LOGGER.info(
        f"Starting notebook execution job with display name: {test_execution_display_name}"
    )

    notebook_service_client = aiplatform_v1.NotebookServiceClient(
        client_options={
            "api_endpoint": test_api_endpoint,
        }
    )

    operation = notebook_service_client.create_notebook_execution_job(
        parent=test_parent,
        notebook_execution_job={
            "display_name": test_execution_display_name,
            # Specify a NotebookRuntimeTemplate to source compute configuration from
            "notebook_runtime_template_resource_name": f"projects/{test_project}/locations/{test_region}/notebookRuntimeTemplates/{test_template}",
            # Specify a Colab Enterprise notebook to run
            "dataform_repository_source": {
                "dataform_repository_resource_name": f"projects/{test_project}/locations/{test_region}/repositories/{test_repository}",
            },
            "gcs_notebook_source": {
                "uri": "gs://e2e-testing-bucket/input/notebooks/spark_connect_e2e_notebook_test.ipynb",
            },
            # Specify a Cloud Storage bucket to store output artifacts
            "gcs_output_uri": "gs://e2e-testing-bucket/output",
            # Run as the service account instead
            "service_account": f"{test_service_account}",
        },
    )
    LOGGER.info("Waiting for operation to complete...")

    result = operation.result()
    LOGGER.info(f"Notebook execution uri: {result}")

    notebook_execution_jobs = (
        notebook_service_client.list_notebook_execution_jobs(parent=test_parent)
    )
    executed_job = list(
        filter(
            lambda job: job.display_name == test_execution_display_name,
            notebook_execution_jobs,
        )
    )

    assert len(executed_job) == 1
    executed_job = executed_job[0]

    LOGGER.info(executed_job)

    LOGGER.info(f"Job status: {executed_job.job_state}")
    assert executed_job.job_state == JobState.JOB_STATE_SUCCEEDED
