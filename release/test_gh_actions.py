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
Unit tests for the gh_actions module.
Run with: python -m pytest release/test_gh_actions.py -v
   or:    cd release && python -m pytest test_gh_actions.py -v
"""

import unittest
from unittest.mock import patch, MagicMock

from github import GithubException

import gh_actions as gh


def _mock_workflow(run_url=None, dispatch_ok=True):
    """Build a mock PyGithub Workflow object."""
    wf = MagicMock()
    wf.create_dispatch.return_value = dispatch_ok

    runs = MagicMock()
    if run_url is None:
        runs.totalCount = 0
    else:
        runs.totalCount = 1
        run = MagicMock()
        run.html_url = run_url
        runs.__getitem__.side_effect = lambda i: run
    wf.get_runs.return_value = runs
    return wf


def _patch_pygithub(workflow=None, raise_on_dispatch=None):
    """
    Patch gh_actions.Github so that .get_repo(...).get_workflow(...) returns the
    given workflow mock. Returns the patcher context.
    """
    if workflow is None:
        workflow = _mock_workflow()
    if raise_on_dispatch is not None:
        workflow.create_dispatch.side_effect = raise_on_dispatch

    repo = MagicMock()
    repo.get_workflow.return_value = workflow
    gh_client = MagicMock()
    gh_client.get_repo.return_value = repo
    return patch("gh_actions.Github", return_value=gh_client), workflow, repo


class TestTriggerWorkflow(unittest.TestCase):

    def setUp(self):
        self._orig_dry_run = gh.DRY_RUN
        gh.DRY_RUN = False

    def tearDown(self):
        gh.DRY_RUN = self._orig_dry_run

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_trigger_workflow_calls_create_dispatch(self):
        patcher, workflow, repo = _patch_pygithub(
            _mock_workflow(run_url="https://github.com/apache/kafka/actions/runs/123"))
        with patcher as mock_gh:
            gh.trigger_workflow("tok", "my_workflow.yml", "main", {"key": "val"})

        mock_gh.assert_called_once_with("tok")
        repo.get_workflow.assert_called_once_with("my_workflow.yml")
        workflow.create_dispatch.assert_called_once_with(ref="main", inputs={"key": "val"})

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_trigger_workflow_uses_configured_repo(self):
        self._orig_repo = gh.GITHUB_REPO
        try:
            gh.GITHUB_REPO = "myuser/kafka-fork"
            patcher, workflow, repo = _patch_pygithub()
            with patcher as mock_gh:
                gh.trigger_workflow("tok", "my_workflow.yml", "main", {})
            mock_gh.return_value.get_repo.assert_called_once_with("myuser/kafka-fork")
        finally:
            gh.GITHUB_REPO = self._orig_repo

    @patch("gh_actions.fail")
    @patch("gh_actions.time.sleep", lambda _: None)
    def test_trigger_workflow_fails_on_github_exception(self, mock_fail):
        err = GithubException(status=404, data={"message": "Not Found"}, headers={})
        patcher, _, _ = _patch_pygithub(raise_on_dispatch=err)
        with patcher:
            gh.trigger_workflow("tok", "my_workflow.yml", "main", {})

        mock_fail.assert_called_once()
        self.assertIn("404", mock_fail.call_args[0][0])
        self.assertIn("my_workflow.yml", mock_fail.call_args[0][0])

    @patch("gh_actions.fail")
    @patch("gh_actions.time.sleep", lambda _: None)
    def test_trigger_workflow_fails_when_dispatch_returns_false(self, mock_fail):
        patcher, _, _ = _patch_pygithub(_mock_workflow(dispatch_ok=False))
        with patcher:
            gh.trigger_workflow("tok", "my_workflow.yml", "main", {})

        mock_fail.assert_called_once()
        self.assertIn("dispatch", mock_fail.call_args[0][0])


class TestLatestRunUrl(unittest.TestCase):

    def setUp(self):
        self._orig_repo = gh.GITHUB_REPO
        gh.GITHUB_REPO = "apache/kafka"

    def tearDown(self):
        gh.GITHUB_REPO = self._orig_repo

    def test_returns_run_url_when_run_exists(self):
        wf = _mock_workflow(run_url="https://github.com/apache/kafka/actions/runs/42")
        url = gh._latest_run_url(wf, "docker_build_and_test.yml")
        self.assertEqual(url, "https://github.com/apache/kafka/actions/runs/42")

    def test_falls_back_when_no_runs(self):
        wf = _mock_workflow(run_url=None)
        url = gh._latest_run_url(wf, "docker_build_and_test.yml")
        self.assertEqual(
            url,
            "https://github.com/apache/kafka/actions/workflows/docker_build_and_test.yml",
        )

    def test_falls_back_on_github_exception(self):
        wf = MagicMock()
        wf.get_runs.side_effect = GithubException(500, {}, {})
        url = gh._latest_run_url(wf, "docker_build_and_test.yml")
        self.assertEqual(
            url,
            "https://github.com/apache/kafka/actions/workflows/docker_build_and_test.yml",
        )


class TestDryRun(unittest.TestCase):

    def setUp(self):
        self._orig_dry_run = gh.DRY_RUN

    def tearDown(self):
        gh.DRY_RUN = self._orig_dry_run

    def test_dry_run_skips_api_call(self):
        gh.DRY_RUN = True
        with patch("gh_actions.Github") as mock_gh:
            gh.trigger_workflow("tok", "test.yml", "main", {"key": "val"})
        mock_gh.assert_not_called()

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_dry_run_false_calls_api(self):
        gh.DRY_RUN = False
        patcher, workflow, _ = _patch_pygithub()
        with patcher as mock_gh:
            gh.trigger_workflow("tok", "test.yml", "main", {"key": "val"})
        mock_gh.assert_called_once()
        workflow.create_dispatch.assert_called_once()


class TestTriggerDockerBuildTest(unittest.TestCase):

    def setUp(self):
        self._orig_dry_run = gh.DRY_RUN
        gh.DRY_RUN = False

    def tearDown(self):
        gh.DRY_RUN = self._orig_dry_run

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_jvm_image(self):
        patcher, workflow, repo = _patch_pygithub()
        with patcher:
            gh.trigger_docker_build_test("tok", "4.3", "jvm", "https://example.com/kafka.tgz")

        repo.get_workflow.assert_called_once_with("docker_build_and_test.yml")
        workflow.create_dispatch.assert_called_once_with(
            ref="4.3",
            inputs={"image_type": "jvm", "kafka_url": "https://example.com/kafka.tgz"},
        )

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_native_image(self):
        patcher, workflow, repo = _patch_pygithub()
        with patcher:
            gh.trigger_docker_build_test("tok", "4.3", "native", "https://example.com/kafka.tgz")

        workflow.create_dispatch.assert_called_once_with(
            ref="4.3",
            inputs={"image_type": "native", "kafka_url": "https://example.com/kafka.tgz"},
        )


class TestTriggerDockerRcRelease(unittest.TestCase):

    def setUp(self):
        self._orig_dry_run = gh.DRY_RUN
        gh.DRY_RUN = False

    def tearDown(self):
        gh.DRY_RUN = self._orig_dry_run

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_jvm_rc_release(self):
        patcher, workflow, repo = _patch_pygithub()
        with patcher:
            gh.trigger_docker_rc_release(
                "tok", "4.3", "jvm", "apache/kafka:4.3.0-rc0",
                "https://example.com/kafka.tgz",
            )

        repo.get_workflow.assert_called_once_with("docker_rc_release.yml")
        workflow.create_dispatch.assert_called_once_with(
            ref="4.3",
            inputs={
                "image_type": "jvm",
                "rc_docker_image": "apache/kafka:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            },
        )

    @patch("gh_actions.time.sleep", lambda _: None)
    def test_native_rc_release(self):
        patcher, workflow, repo = _patch_pygithub()
        with patcher:
            gh.trigger_docker_rc_release(
                "tok", "4.3", "native", "apache/kafka-native:4.3.0-rc0",
                "https://example.com/kafka.tgz",
            )

        workflow.create_dispatch.assert_called_once_with(
            ref="4.3",
            inputs={
                "image_type": "native",
                "rc_docker_image": "apache/kafka-native:4.3.0-rc0",
                "kafka_url": "https://example.com/kafka.tgz",
            },
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
            f"gh_actions.trigger_docker_build_test sends {sent} but workflow expects {expected}")

    def test_rc_release_inputs_match(self):
        expected = self._load_workflow_inputs("docker_rc_release.yml")
        sent = {"image_type", "rc_docker_image", "kafka_url"}
        self.assertEqual(sent, expected,
            f"gh_actions.trigger_docker_rc_release sends {sent} but workflow expects {expected}")


# We need to import trigger_docker_workflows from release.py, but that file
# executes interactively at module level. So we import it directly from the
# function definition using importlib to avoid running the top-level code.
def _load_trigger_docker_workflows():
    """
    Extract trigger_docker_workflows from release.py without executing the
    module's top-level interactive code. We parse the source and compile just
    the function definition.
    """
    import os
    release_path = os.path.join(os.path.dirname(__file__), "release.py")
    with open(release_path) as f:
        source = f.read()

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

    ns = {
        'gh_actions': gh,
        'confirm': None,
        'confirm_or_fail': None,
        'preferences': None,
        'templates': None,
        'prompt': None,
    }
    exec(compile(func_source, release_path, 'exec'), ns)
    return ns['trigger_docker_workflows'], ns


_trigger_fn, _fn_namespace = _load_trigger_docker_workflows()


class TestTriggerDockerWorkflows(unittest.TestCase):
    """Test the trigger_docker_workflows function from release.py."""

    def setUp(self):
        self._orig_dry_run = gh.DRY_RUN
        gh.DRY_RUN = True  # Always dry-run in tests to skip the PyGithub call

    def tearDown(self):
        gh.DRY_RUN = self._orig_dry_run

    def _run(self, confirm_responses):
        """
        Run trigger_docker_workflows with mocked interactive prompts.
        Returns the list of (workflow_file, ref, inputs) calls captured from
        gh_actions.trigger_workflow.
        """
        confirm_iter = iter(confirm_responses)
        _fn_namespace['confirm'] = lambda msg: next(confirm_iter)
        _fn_namespace['confirm_or_fail'] = lambda msg: None
        _fn_namespace['preferences'] = MagicMock()
        _fn_namespace['preferences'].get = MagicMock(return_value="fake-token")
        _fn_namespace['templates'] = MagicMock()
        _fn_namespace['templates'].github_token_instructions = MagicMock(return_value="token instructions")
        _fn_namespace['prompt'] = MagicMock(return_value="fake-token")

        with patch("gh_actions.trigger_workflow") as mock_trigger:
            _trigger_fn("4.3.0-rc0", "4.3.0", "4.3")
            return [c[0] for c in mock_trigger.call_args_list]

    def test_happy_path_all_yes(self):
        # confirm calls: 1) trigger? yes, 2) builds passed? yes
        calls = self._run([True, True])
        self.assertEqual(len(calls), 4)
        # First 2: build_and_test (jvm, native)
        self.assertEqual(calls[0][1], "docker_build_and_test.yml")
        self.assertEqual(calls[1][1], "docker_build_and_test.yml")
        # Last 2: rc_release (jvm, native)
        self.assertEqual(calls[2][1], "docker_rc_release.yml")
        self.assertEqual(calls[3][1], "docker_rc_release.yml")

    def test_skip_docker_workflows(self):
        calls = self._run([False])
        self.assertEqual(calls, [])

    def test_cve_retry_then_pass(self):
        # confirm: 1) trigger? yes, 2) passed? no, 3) passed? yes
        calls = self._run([True, False, True])
        # 2 build_test (1st) + 2 build_test (retry) + 2 rc_release = 6
        self.assertEqual(len(calls), 6)
        for i in range(4):
            self.assertEqual(calls[i][1], "docker_build_and_test.yml")
        self.assertEqual(calls[4][1], "docker_rc_release.yml")
        self.assertEqual(calls[5][1], "docker_rc_release.yml")

    def test_multiple_cve_retries(self):
        # confirm: 1) trigger? yes, 2) no, 3) no, 4) yes
        calls = self._run([True, False, False, True])
        # 3 rounds of build_test (6) + 1 round of rc_release (2) = 8
        self.assertEqual(len(calls), 8)

    def test_rc_release_uses_correct_image_names(self):
        calls = self._run([True, True])
        # call[2] is RC release JVM, call[3] is RC release native
        jvm_inputs = calls[2][3]
        self.assertEqual(jvm_inputs["rc_docker_image"], "apache/kafka:4.3.0-rc0")
        self.assertEqual(jvm_inputs["image_type"], "jvm")

        native_inputs = calls[3][3]
        self.assertEqual(native_inputs["rc_docker_image"], "apache/kafka-native:4.3.0-rc0")
        self.assertEqual(native_inputs["image_type"], "native")

    def test_kafka_url_construction(self):
        calls = self._run([True, True])
        expected_url = "https://dist.apache.org/repos/dist/dev/kafka/4.3.0-rc0/kafka_2.13-4.3.0.tgz"
        self.assertEqual(calls[0][3]["kafka_url"], expected_url)

    def test_dev_branch_used_as_ref(self):
        calls = self._run([True, True])
        for call in calls:
            self.assertEqual(call[2], "4.3")


if __name__ == "__main__":
    unittest.main()
