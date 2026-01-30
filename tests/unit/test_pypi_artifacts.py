import json
import os
import unittest

from packaging.requirements import InvalidRequirement

from google.cloud.dataproc_spark_connect.pypi_artifacts import PyPiArtifacts


class PyPiArtifactsTest(unittest.TestCase):

    @staticmethod
    def test_valid_inputs():
        file = PyPiArtifacts(
            {
                "spacy",
                "spacy==1.2.3",
                "spacy>=1.2",
                "spacy==1.2.*",
                "spacy==1.*",
                "abc.whl",
            }
        ).write_packages_config("uuid")
        os.remove(file)

    def test_bad_format(self):
        with self.assertRaisesRegex(
            InvalidRequirement,
            "Expected semicolon \(after name and no valid version specifier\).*",
        ):
            PyPiArtifacts({"pypi://spacy:23"})

    def test_validate_file_content(self):
        file_path = PyPiArtifacts(
            {"spacy==1.2", "pkg>=1.2", "abc"}
        ).write_packages_config("uuid")
        actual = json.load(open(file_path))
        self.assertEqual("0.5", actual["version"])
        self.assertEqual("PYPI", actual["packageType"])
        self.assertEqual(
            ["abc", "pkg>=1.2", "spacy==1.2"], sorted(actual["packages"])
        )

    def test_validate_file_name_pattern(self):
        file_path = PyPiArtifacts({"spacy==1.2"}).write_packages_config("uuid")
        file_name = os.path.basename(file_path)
        self.assertTrue(file_name.startswith("add-artifacts-1729-"))
        self.assertTrue(file_name.endswith(".json"))
