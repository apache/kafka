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


def _post_calls(mock_api):
    """Filter mock_api calls to only POST (dispatch) calls, ignoring GET (run URL lookup) calls."""
    return [c for c in mock_api.call_args_list if c[0][1] == "POST"]


class TestTriggerWorkflow(unittest.TestCase):

    @patch("github._api_request")
    def test_trigger_workflow_calls_correct_endpoint(self, mock_api):
        github.trigger_workflow("tok", "my_workflow.yml", "main", {"key": "val"})

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][0], (
            "tok", "POST",
            f"/repos/{github.GITHUB_REPO}/actions/workflows/my_workflow.yml/dispatches",
            {"ref": "main", "inputs": {"key": "val"}},
        ))

    @patch("github._api_request")
    def test_trigger_workflow_fetches_run_url(self, mock_api):
        """Verify that a GET call is made to fetch the latest run URL."""
        github.trigger_workflow("tok", "my_workflow.yml", "main", {"key": "val"})

        get_calls = [c for c in mock_api.call_args_list if c[0][1] == "GET"]
        self.assertEqual(len(get_calls), 1)
        self.assertIn("/runs?per_page=1", get_calls[0][0][2])


class TestTriggerDockerBuildTest(unittest.TestCase):

    @patch("github._api_request")
    def test_jvm_image(self, mock_api):
        github.trigger_docker_build_test("tok", "4.3", "jvm", "https://example.com/kafka.tgz")

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][0], (
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_build_and_test.yml/dispatches",
            {"ref": "4.3", "inputs": {"image_type": "jvm", "kafka_url": "https://example.com/kafka.tgz"}},
        ))

    @patch("github._api_request")
    def test_native_image(self, mock_api):
        github.trigger_docker_build_test("tok", "4.3", "native", "https://example.com/kafka.tgz")

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][0], (
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_build_and_test.yml/dispatches",
            {"ref": "4.3", "inputs": {"image_type": "native", "kafka_url": "https://example.com/kafka.tgz"}},
        ))


class TestTriggerDockerRcRelease(unittest.TestCase):

    @patch("github._api_request")
    def test_jvm_rc_release(self, mock_api):
        github.trigger_docker_rc_release(
            "tok", "4.3", "jvm", "apache/kafka:4.3.0-rc0", "https://example.com/kafka.tgz"
        )

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][0], (
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_rc_release.yml/dispatches",
            {"ref": "4.3", "inputs": {
                "image_type": "jvm",
                "rc_docker_image": "apache/kafka:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            }},
        ))

    @patch("github._api_request")
    def test_native_rc_release(self, mock_api):
        github.trigger_docker_rc_release(
            "tok", "4.3", "native", "apache/kafka-native:4.3.0-rc0", "https://example.com/kafka.tgz"
        )

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0][0], (
            "tok", "POST",
            "/repos/apache/kafka/actions/workflows/docker_rc_release.yml/dispatches",
            {"ref": "4.3", "inputs": {
                "image_type": "native",
                "rc_docker_image": "apache/kafka-native:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            }},
        ))


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

        # 4 POST (dispatch) + 4 GET (run URL lookup) = 8 total
        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 4)

        # Build test JVM
        self.assertIn("docker_build_and_test.yml", posts[0][0][2])
        self.assertEqual(posts[0][0][3]["inputs"]["image_type"], "jvm")

        # Build test native
        self.assertIn("docker_build_and_test.yml", posts[1][0][2])
        self.assertEqual(posts[1][0][3]["inputs"]["image_type"], "native")

        # RC release JVM
        self.assertIn("docker_rc_release.yml", posts[2][0][2])
        self.assertEqual(posts[2][0][3]["inputs"]["rc_docker_image"], "apache/kafka:4.3.0-rc0")

        # RC release native
        self.assertIn("docker_rc_release.yml", posts[3][0][2])
        self.assertEqual(posts[3][0][3]["inputs"]["rc_docker_image"], "apache/kafka-native:4.3.0-rc0")


# We need to import trigger_docker_workflows from release.py, but that file
# executes interactively at module level. So we import it directly from the
# function definition using importlib to avoid running the top-level code.
def _load_trigger_docker_workflows():
    """
    Extract trigger_docker_workflows from release.py without executing the
    module's top-level interactive code. We parse the source and compile just
    the function definition.
    """
    import os, types
    release_path = os.path.join(os.path.dirname(__file__), "release.py")
    with open(release_path) as f:
        source = f.read()

    # Extract the function source (from 'def trigger_docker_workflows' to the
    # next top-level def or non-indented statement)
    lines = source.split('\n')
    func_lines = []
    capturing = False
    for line in lines:
        if line.startswith('def trigger_docker_workflows('):
            capturing = True
        elif capturing and line and not line[0].isspace() and not line.startswith('#'):
            break
        if capturing:
            func_lines.append(line)

    func_source = '\n'.join(func_lines)

    # Create a module-like namespace with the dependencies the function needs
    ns = {
        'github': github,
        'confirm': None,          # will be mocked per test
        'confirm_or_fail': None,  # will be mocked per test
        'preferences': None,      # will be mocked per test
        'templates': None,        # will be mocked per test
        'prompt': None,           # will be mocked per test
    }
    exec(compile(func_source, release_path, 'exec'), ns)
    return ns['trigger_docker_workflows'], ns


_trigger_fn, _fn_namespace = _load_trigger_docker_workflows()


class TestTriggerDockerWorkflows(unittest.TestCase):
    """Test the trigger_docker_workflows function from release.py."""

    def setUp(self):
        self._orig_dry_run = github.DRY_RUN
        github.DRY_RUN = True  # Always dry-run in tests

    def tearDown(self):
        github.DRY_RUN = self._orig_dry_run

    def _run(self, confirm_responses, confirm_or_fail_responses=None):
        """
        Run trigger_docker_workflows with mocked interactive prompts.
        confirm_responses: list of booleans for each confirm() call
        confirm_or_fail_responses: list of None (success) values for confirm_or_fail()
        """
        confirm_iter = iter(confirm_responses)
        _fn_namespace['confirm'] = lambda msg: next(confirm_iter)
        _fn_namespace['confirm_or_fail'] = lambda msg: None  # always succeeds
        _fn_namespace['preferences'] = MagicMock()
        _fn_namespace['preferences'].get = MagicMock(return_value="fake-token")
        _fn_namespace['templates'] = MagicMock()
        _fn_namespace['templates'].github_token_instructions = MagicMock(return_value="token instructions")
        _fn_namespace['prompt'] = MagicMock(return_value="fake-token")

        with patch("github._api_request") as mock_api:
            _trigger_fn("4.3.0-rc0", "4.3.0", "4.3")
            return mock_api

    def test_happy_path_all_yes(self):
        """User says yes to everything, builds pass first time."""
        # confirm calls: 1) trigger? yes, 2) builds passed? yes
        mock_api = self._run([True, True])

        posts = _post_calls(mock_api)
        self.assertEqual(len(posts), 4)

        # First 2: build_and_test (jvm, native)
        self.assertIn("docker_build_and_test.yml", posts[0][0][2])
        self.assertIn("docker_build_and_test.yml", posts[1][0][2])
        # Last 2: rc_release (jvm, native)
        self.assertIn("docker_rc_release.yml", posts[2][0][2])
        self.assertIn("docker_rc_release.yml", posts[3][0][2])

    def test_skip_docker_workflows(self):
        """User declines to trigger Docker workflows."""
        # confirm calls: 1) trigger? no
        mock_api = self._run([False])

        mock_api.assert_not_called()

    def test_cve_retry_then_pass(self):
        """CVEs found on first attempt, user retries, second attempt passes."""
        # confirm calls: 1) trigger? yes, 2) builds passed? no (CVE found),
        #                3) builds passed? yes (after retry)
        mock_api = self._run([True, False, True])

        posts = _post_calls(mock_api)
        # 2 build_test (1st) + 2 build_test (retry) + 2 rc_release = 6
        self.assertEqual(len(posts), 6)

        # First 4: build_and_test (2 attempts x 2 image types)
        for i in range(4):
            self.assertIn("docker_build_and_test.yml", posts[i][0][2])
        # Last 2: rc_release
        self.assertIn("docker_rc_release.yml", posts[4][0][2])
        self.assertIn("docker_rc_release.yml", posts[5][0][2])

    def test_multiple_cve_retries(self):
        """CVEs found twice, third attempt passes."""
        # confirm calls: 1) trigger? yes, 2) passed? no, 3) passed? no, 4) passed? yes
        mock_api = self._run([True, False, False, True])

        posts = _post_calls(mock_api)
        # 3 rounds of build_test (6) + 1 round of rc_release (2) = 8
        self.assertEqual(len(posts), 8)

    def test_rc_release_uses_correct_image_names(self):
        """Verify JVM uses apache/kafka and native uses apache/kafka-native."""
        mock_api = self._run([True, True])

        posts = _post_calls(mock_api)
        # RC release JVM (3rd POST)
        jvm_inputs = posts[2][0][3]["inputs"]
        self.assertEqual(jvm_inputs["rc_docker_image"], "apache/kafka:4.3.0-rc0")
        self.assertEqual(jvm_inputs["image_type"], "jvm")

        # RC release native (4th POST)
        native_inputs = posts[3][0][3]["inputs"]
        self.assertEqual(native_inputs["rc_docker_image"], "apache/kafka-native:4.3.0-rc0")
        self.assertEqual(native_inputs["image_type"], "native")

    def test_kafka_url_construction(self):
        """Verify the kafka_url is constructed correctly from rc_tag and release_version."""
        mock_api = self._run([True, True])

        expected_url = "https://dist.apache.org/repos/dist/dev/kafka/4.3.0-rc0/kafka_2.13-4.3.0.tgz"
        posts = _post_calls(mock_api)
        self.assertEqual(posts[0][0][3]["inputs"]["kafka_url"], expected_url)

    def test_dev_branch_used_as_ref(self):
        """Verify dev_branch is passed as the workflow ref."""
        mock_api = self._run([True, True])

        posts = _post_calls(mock_api)
        for post in posts:
            self.assertEqual(post[0][3]["ref"], "4.3")


if __name__ == "__main__":
    unittest.main()
