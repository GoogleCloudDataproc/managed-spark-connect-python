# Copyright 2024 Google LLC
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
from setuptools import find_namespace_packages, setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


setup(
    name="google-cloud-spark-connect",
    version="2.2.0",
    description="Managed Spark client library for Spark Connect",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Google LLC",
    url="https://github.com/GoogleCloudDataproc/managed-spark-connect-python",
    license="Apache 2.0",
    packages=find_namespace_packages(include=["google.*"]),
    install_requires=[
        "google-api-core>=2.19",
        "google-cloud-dataproc>=5.18",
        "packaging>=20.0",
        "tqdm>=4.67",
        "websockets>=14.0",
    ],
    extras_require={
        # client and spark are mutually exclusive: install at most one. Both
        # packages write to the same pyspark module directory, so installing
        # both risks version skew, and uninstalling either breaks the other.
        # Extras are additive, so pip cannot enforce this. With neither, the
        # package uses whichever pyspark the environment already provides.
        "client": [
            "pyspark-client~=4.0.0",
        ],
        "spark": [
            "pyspark[connect]~=4.0.0",
        ],
        # Everything here is optional at runtime: managed_spark_connect._ipython
        # only initializes the extras whose dependencies are importable.
        "interactive": [
            "google-colabsqlviz>=0.4.0",
            # Imported directly by managed_spark_connect._ipython and
            # managed_spark_magics; previously these only arrived transitively
            # via google-colabsqlviz.
            "ipython>=7.34.0",
            # Statement splitting and parameter detection in %%sparksql.
            "sqlparse>=0.5.0",
            "traitlets>=5.1",
        ],
    },
)
