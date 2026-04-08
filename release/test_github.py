#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for the github module.
Run with: python -m pytest release/test_github.py -v
   or:    cd release && python -m pytest test_github.py -v
"""

import json
import urllib.error
import unittest
from unittest.mock import patch, MagicMock

import github


class TestApiRequest(unittest.TestCase):

    def _mock_response(self, status=204, body=None):
        mock_resp = MagicMock()
        mock_resp.status = status
        mock_resp.read.return_value = json.dumps(body).encode("utf-8") if body else b""
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    @patch("urllib.request.urlopen")
    def test_post_request_204_returns_none(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=204)

        result = github._api_request("my-token", "POST", "/test/path", {"key": "val"})

        self.assertIsNone(result)
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "https://api.github.com/test/path")
        self.assertEqual(req.get_method(), "POST")

    @patch("urllib.request.urlopen")
    def test_get_request_200_returns_json(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=200, body={"id": 42})

        result = github._api_request("my-token", "GET", "/repos/test")

        self.assertEqual(result, {"id": 42})

    @patch("urllib.request.urlopen")
    def test_request_sets_auth_header(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=204)

        github._api_request("secret-token", "POST", "/path", {"a": 1})

        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_header("Authorization"), "token secret-token")

    @patch("urllib.request.urlopen")
    def test_request_sets_accept_header(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=204)

        github._api_request("tok", "POST", "/path", {"a": 1})

        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_header("Accept"), "application/vnd.github.v3+json")

    @patch("urllib.request.urlopen")
    def test_request_sets_content_type_when_body_present(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=204)

        github._api_request("tok", "POST", "/path", {"a": 1})

        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_header("Content-type"), "application/json")

    @patch("urllib.request.urlopen")
    def test_request_no_content_type_when_no_body(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=200, body={"ok": True})

        github._api_request("tok", "GET", "/path")

        req = mock_urlopen.call_args[0][0]
        self.assertIsNone(req.get_header("Content-type"))
        self.assertIsNone(req.data)

    @patch("urllib.request.urlopen")
    def test_request_serializes_body_as_json(self, mock_urlopen):
        mock_urlopen.return_value = self._mock_response(status=204)

        github._api_request("tok", "POST", "/path", {"ref": "main", "inputs": {"k": "v"}})

        req = mock_urlopen.call_args[0][0]
        self.assertEqual(json.loads(req.data), {"ref": "main", "inputs": {"k": "v"}})

    @patch("github.fail")
    @patch("urllib.request.urlopen")
    def test_http_error_calls_fail(self, mock_urlopen, mock_fail):
        error_body = b'{"message": "Not Found"}'
        mock_fp = MagicMock()
        mock_fp.read.return_value = error_body
        http_error = urllib.error.HTTPError(
            url="https://api.github.com/test",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=mock_fp,
        )
        mock_urlopen.side_effect = http_error

        github._api_request("tok", "GET", "/test")

        mock_fail.assert_called_once()
        fail_msg = mock_fail.call_args[0][0]
        self.assertIn("404", fail_msg)
        self.assertIn("GET", fail_msg)
        self.assertIn("/test", fail_msg)


class TestDryRun(unittest.TestCase):

    def setUp(self):
        self._orig_dry_run = github.DRY_RUN
        self._orig_repo = github.GITHUB_REPO

    def tearDown(self):
        github.DRY_RUN = self._orig_dry_run
        github.GITHUB_REPO = self._orig_repo

    @patch("urllib.request.urlopen")
    def test_dry_run_skips_http_call(self, mock_urlopen):
        github.DRY_RUN = True

        result = github._api_request("tok", "POST", "/test", {"key": "val"})

        self.assertIsNone(result)
        mock_urlopen.assert_not_called()

    @patch("urllib.request.urlopen")
    def test_dry_run_false_makes_http_call(self, mock_urlopen):
        github.DRY_RUN = False
        mock_resp = MagicMock()
        mock_resp.status = 204
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        github._api_request("tok", "POST", "/test", {"key": "val"})

        mock_urlopen.assert_called_once()


class TestConfigurableRepo(unittest.TestCase):

    def setUp(self):
        self._orig_repo = github.GITHUB_REPO

    def tearDown(self):
        github.GITHUB_REPO = self._orig_repo

    @patch("github._api_request")
    def test_custom_repo_in_workflow_path(self, mock_api):
        github.GITHUB_REPO = "myuser/kafka-fork"

        github.trigger_workflow("tok", "test.yml", "main", {"k": "v"})

        path = mock_api.call_args[0][2]
        self.assertIn("myuser/kafka-fork", path)
        self.assertNotIn("apache/kafka", path)

    @patch("github._api_request")
    def test_default_repo_is_apache_kafka(self, mock_api):
        github.GITHUB_REPO = "apache/kafka"

        github.trigger_workflow("tok", "test.yml", "main", {"k": "v"})

        path = mock_api.call_args[0][2]
        self.assertIn("apache/kafka", path)


class TestTriggerWorkflow(unittest.TestCase):

    @patch("github._api_request")
    def test_trigger_workflow_calls_correct_endpoint(self, mock_api):
        github.trigger_workflow("tok", "my_workflow.yml", "main", {"key": "val"})

        mock_api.assert_called_once_with(
            "tok", "POST",
            f"/repos/{github.GITHUB_REPO}/actions/workflows/my_workflow.yml/dispatches",
            {"ref": "main", "inputs": {"key": "val"}},
        )


class TestTriggerDockerBuildTest(unittest.TestCase):

    @patch("github._api_request")
    def test_jvm_image(self, mock_api):
        github.trigger_docker_build_test("tok", "4.3", "jvm", "https://example.com/kafka.tgz")

        mock_api.assert_called_once_with(
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_build_and_test.yml/dispatches",
            {"ref": "4.3", "inputs": {"image_type": "jvm", "kafka_url": "https://example.com/kafka.tgz"}},
        )

    @patch("github._api_request")
    def test_native_image(self, mock_api):
        github.trigger_docker_build_test("tok", "4.3", "native", "https://example.com/kafka.tgz")

        mock_api.assert_called_once_with(
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_build_and_test.yml/dispatches",
            {"ref": "4.3", "inputs": {"image_type": "native", "kafka_url": "https://example.com/kafka.tgz"}},
        )


class TestTriggerDockerRcRelease(unittest.TestCase):

    @patch("github._api_request")
    def test_jvm_rc_release(self, mock_api):
        github.trigger_docker_rc_release(
            "tok", "4.3", "jvm", "apache/kafka:4.3.0-rc0", "https://example.com/kafka.tgz"
        )

        mock_api.assert_called_once_with(
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_rc_release.yml/dispatches",
            {"ref": "4.3", "inputs": {
                "image_type": "jvm",
                "rc_docker_image": "apache/kafka:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            }},
        )

    @patch("github._api_request")
    def test_native_rc_release(self, mock_api):
        github.trigger_docker_rc_release(
            "tok", "4.3", "native", "apache/kafka-native:4.3.0-rc0", "https://example.com/kafka.tgz"
        )

        mock_api.assert_called_once_with(
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_rc_release.yml/dispatches",
            {"ref": "4.3", "inputs": {
                "image_type": "native",
                "rc_docker_image": "apache/kafka-native:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            }},
        )


class TestWorkflowInputAlignment(unittest.TestCase):
    """Verify that the inputs we send match what the workflow YAML files expect."""

    def _load_workflow_inputs(self, workflow_file):
        import yaml
        import os
        base = os.path.join(os.path.dirname(__file__), "..", ".github", "workflows")
        with open(os.path.join(base, workflow_file)) as f:
            wf = yaml.safe_load(f)
        # PyYAML parses 'on' as boolean True
        return set(wf[True]["workflow_dispatch"]["inputs"].keys())

    def test_build_and_test_inputs_match(self):
        expected = self._load_workflow_inputs("docker_build_and_test.yml")
        sent = {"image_type", "kafka_url"}
        self.assertEqual(sent, expected,
            f"github.trigger_docker_build_test sends {sent} but workflow expects {expected}")

    def test_rc_release_inputs_match(self):
        expected = self._load_workflow_inputs("docker_rc_release.yml")
        sent = {"image_type", "rc_docker_image", "kafka_url"}
        self.assertEqual(sent, expected,
            f"github.trigger_docker_rc_release sends {sent} but workflow expects {expected}")


class TestReleaseScriptIntegration(unittest.TestCase):
    """Simulate the exact flow that release.py uses to trigger Docker workflows."""

    @patch("github._api_request")
    def test_full_release_flow(self, mock_api):
        release_version = "4.3.0"
        rc_tag = "4.3.0-rc0"
        dev_branch = "4.3"
        kafka_url = f"https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka_2.13-{release_version}.tgz"

        # Step 1: Build & test for both image types (as release.py does)
        for image_type in ["jvm", "native"]:
            github.trigger_docker_build_test("tok", dev_branch, image_type, kafka_url)

        # Step 2: RC release for both image types (as release.py does)
        for image_type in ["jvm", "native"]:
            docker_image_name = "apache/kafka-native" if image_type == "native" else "apache/kafka"
            rc_docker_image = f"{docker_image_name}:{rc_tag}"
            github.trigger_docker_rc_release("tok", dev_branch, image_type, rc_docker_image, kafka_url)

        self.assertEqual(mock_api.call_count, 4)

        # Verify each call's workflow file and inputs
        calls = mock_api.call_args_list

        # Build test JVM
        self.assertIn("docker_build_and_test.yml", calls[0][0][2])
        self.assertEqual(calls[0][0][3]["inputs"]["image_type"], "jvm")

        # Build test native
        self.assertIn("docker_build_and_test.yml", calls[1][0][2])
        self.assertEqual(calls[1][0][3]["inputs"]["image_type"], "native")

        # RC release JVM
        self.assertIn("docker_rc_release.yml", calls[2][0][2])
        self.assertEqual(calls[2][0][3]["inputs"]["rc_docker_image"], "apache/kafka:4.3.0-rc0")

        # RC release native
        self.assertIn("docker_rc_release.yml", calls[3][0][2])
        self.assertEqual(calls[3][0][3]["inputs"]["rc_docker_image"], "apache/kafka-native:4.3.0-rc0")


if __name__ == "__main__":
    unittest.main()
