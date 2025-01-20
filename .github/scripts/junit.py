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

import argparse
from collections import OrderedDict
import dataclasses
from functools import partial
from glob import glob
import logging
import os
import os.path
import pathlib
import re
import sys
from typing import Tuple, Optional, List, Iterable
import xml.etree.ElementTree
import html

import yaml


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stderr)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

PASSED = "PASSED ✅"
FAILED = "FAILED ❌"
FLAKY = "FLAKY ⚠️ "
SKIPPED = "SKIPPED 🙈"
QUARANTINED = "QUARANTINED 😷"


def get_env(key: str, fn = str) -> Optional:
    value = os.getenv(key)
    if value is None:
        logger.debug(f"Could not find env {key}")
        return None
    else:
        logger.debug(f"Read env {key}: {value}")
        return fn(value)


@dataclasses.dataclass
class TestCase:
    test_name: str
    class_name: str
    time: float
    failure_message: Optional[str]
    failure_class: Optional[str]
    failure_stack_trace: Optional[str]

    def key(self) -> Tuple[str, str]:
        return self.class_name, self.test_name

    def __repr__(self):
        return f"{self.class_name} {self.test_name}"


@dataclasses.dataclass
class TestSuite:
    name: str
    path: str
    tests: int
    skipped: int
    failures: int
    errors: int
    time: float
    failed_tests: List[TestCase]
    skipped_tests: List[TestCase]
    passed_tests: List[TestCase]


# Java method names can start with alpha, "_", or "$". Following characters can also include digits
method_matcher = re.compile(r"([a-zA-Z_$][a-zA-Z0-9_$]+).*")


def clean_test_name(test_name: str) -> str:
    cleaned = test_name.strip("\"").rstrip("()")
    m = method_matcher.match(cleaned)
    return m.group(1)


class TestCatalogExporter:
    def __init__(self):
        self.all_tests = {}   # module -> class -> set of methods

    def handle_suite(self, module: str, suite: TestSuite):
        if module not in self.all_tests:
            self.all_tests[module] = OrderedDict()

        for test in suite.failed_tests:
            if test.class_name not in self.all_tests[module]:
                self.all_tests[module][test.class_name] = set()
            self.all_tests[module][test.class_name].add(clean_test_name(test.test_name))
        for test in suite.passed_tests:
            if test.class_name not in self.all_tests[module]:
                self.all_tests[module][test.class_name] = set()
            self.all_tests[module][test.class_name].add(clean_test_name(test.test_name))

    def export(self, out_dir: str):
        if not os.path.exists(out_dir):
            logger.debug(f"Creating output directory {out_dir} for test catalog export.")
            os.makedirs(out_dir)

        total_count = 0
        for module, module_tests in self.all_tests.items():
            module_path = os.path.join(out_dir, module)
            if not os.path.exists(module_path):
                os.makedirs(module_path)

            sorted_tests = {}
            count = 0
            for test_class, methods in module_tests.items():
                sorted_methods = sorted(methods)
                count += len(sorted_methods)
                sorted_tests[test_class] = sorted_methods

            out_path = os.path.join(module_path, f"tests.yaml")
            logger.debug(f"Writing {count} tests for {module} into {out_path}.")
            total_count += count
            with open(out_path, "w") as fp:
                yaml.dump(sorted_tests, fp)

        logger.debug(f"Wrote {total_count} tests into test catalog.")


def parse_report(workspace_path, report_path, fp) -> Iterable[TestSuite]:
    cur_suite: Optional[TestSuite] = None
    partial_test_case = None
    test_case_failed = False
    test_case_skipped = False
    for (event, elem) in xml.etree.ElementTree.iterparse(fp, events=["start", "end"]):
        if event == "start":
            if elem.tag == "testsuite":
                name = elem.get("name")
                tests = int(elem.get("tests", 0))
                skipped = int(elem.get("skipped", 0))
                failures = int(elem.get("failures", 0))
                errors = int(elem.get("errors", 0))
                suite_time = float(elem.get("time", 0.0))
                cur_suite = TestSuite(name, report_path, tests, skipped, failures, errors, suite_time, [], [], [])
            elif elem.tag == "testcase":
                test_name = elem.get("name")
                class_name = elem.get("classname")
                test_time = float(elem.get("time", 0.0))
                partial_test_case = partial(TestCase, test_name, class_name, test_time)
                test_case_failed = False
                test_case_skipped = False
            elif elem.tag == "failure":
                failure_message = elem.get("message")
                if failure_message:
                    failure_message = html.escape(failure_message)
                    failure_message = failure_message.replace('\n', '<br>').replace('\r', '<br>')
                failure_class = elem.get("type")
                failure_stack_trace = elem.text
                failure = partial_test_case(failure_message, failure_class, failure_stack_trace)
                cur_suite.failed_tests.append(failure)
                test_case_failed = True
            elif elem.tag == "skipped":
                skipped = partial_test_case(None, None, None)
                cur_suite.skipped_tests.append(skipped)
                test_case_skipped = True
            else:
                pass
        elif event == "end":
            if elem.tag == "testcase":
                if not test_case_failed and not test_case_skipped:
                    passed = partial_test_case(None, None, None)
                    cur_suite.passed_tests.append(passed)
                partial_test_case = None
            elif elem.tag == "testsuite":
                yield cur_suite
                cur_suite = None
        else:
            logger.error(f"Unhandled xml event {event}: {elem}")


def pretty_time_duration(seconds: float) -> str:
    time_min, time_sec = divmod(int(seconds), 60)
    time_hour, time_min = divmod(time_min, 60)
    time_fmt = ""
    if time_hour > 0:
        time_fmt += f"{time_hour}h"
    if time_min > 0:
        time_fmt += f"{time_min}m"
    time_fmt += f"{time_sec}s"
    return time_fmt


def split_report_path(base_path: str, report_path: str) -> Tuple[str, str]:
    """
    Parse a report XML and extract the module path. Test report paths look like:

        build/junit-xml/module[/sub-module]/[task]/TEST-class.method.xml

    This method strips off a base path and assumes all path segments leading up to the suite name
    are part of the module path.

    Returns a tuple of (module, task)
    """
    rel_report_path = os.path.relpath(report_path, base_path)
    path_segments = pathlib.Path(rel_report_path).parts

    return os.path.join(*path_segments[0:-2]), path_segments[-2]


if __name__ == "__main__":
    """
    Parse JUnit XML reports and generate GitHub job summary in Markdown format.

    A Markdown summary of the test results is written to stdout. This should be redirected to $GITHUB_STEP_SUMMARY
    within the action. Additional debug logs are written to stderr.

    Exits with status code 0 if no tests failed, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description="Parse JUnit XML results.")
    parser.add_argument("--path",
                        required=False,
                        default="build/junit-xml",
                        help="Base path of JUnit XML files. A glob of **/*.xml will be applied on top of this path.")
    parser.add_argument("--export-test-catalog",
                        required=False,
                        default="",
                        help="Optional path to dump all tests")

    if not os.getenv("GITHUB_WORKSPACE"):
        print("This script is intended to by run by GitHub Actions.")
        exit(1)

    args = parser.parse_args()

    glob_path = os.path.join(args.path, "**/*.xml")
    reports = glob(pathname=glob_path, recursive=True)
    logger.info(f"Found {len(reports)} JUnit results")
    workspace_path = get_env("GITHUB_WORKSPACE") # e.g., /home/runner/work/apache/kafka

    total_file_count = 0
    total_run = 0       # All test runs according to <testsuite tests="N"/>
    total_skipped = 0   # All skipped tests according to <testsuite skipped="N"/>
    total_errors = 0    # All test errors according to <testsuite errors="N"/>
    total_time = 0      # All test time according to <testsuite time="N"/>
    total_failures = 0  # All unique test names that only failed. Re-run tests not counted
    total_flaky = 0     # All unique test names that failed and succeeded
    total_success = 0   # All unique test names that only succeeded. Re-runs not counted
    total_tests = 0     # All unique test names that were run. Re-runs not counted

    failed_table = []
    flaky_table = []
    skipped_table = []
    quarantined_table = []

    exporter = TestCatalogExporter()

    logger.debug(f"::group::Parsing {len(reports)} JUnit Report Files")
    for report in reports:
        with open(report, "r") as fp:
            module_path, task = split_report_path(args.path, report)
            logger.debug(f"Parsing file: {report}, module: {module_path}, task: {task}")
            for suite in parse_report(workspace_path, report, fp):
                total_skipped += suite.skipped
                total_errors += suite.errors
                total_time += suite.time
                total_run += suite.tests

                # Due to how the Develocity Test Retry plugin interacts with our generated ClusterTests, we can see
                # tests pass and then fail in the same run. Because of this, we need to capture all passed and all
                # failed for each suite. Then we can find flakes by taking the intersection of those two.
                all_suite_passed = {test.key(): test for test in suite.passed_tests}
                all_suite_failed = {test.key(): test for test in suite.failed_tests}
                flaky = all_suite_passed.keys() & all_suite_failed.keys()
                all_tests = all_suite_passed.keys() | all_suite_failed.keys()
                total_tests += len(all_tests)
                total_flaky += len(flaky)
                total_failures += len(all_suite_failed) - len(flaky)
                total_success += len(all_suite_passed) - len(flaky)

                # Display failures first. Iterate across the unique failed tests to avoid duplicates in table.
                for test_failure in all_suite_failed.values():
                    if test_failure.key() in flaky:
                        continue
                    logger.debug(f"Found test failure: {test_failure}")
                    simple_class_name = test_failure.class_name.split(".")[-1]
                    failed_table.append((simple_class_name, test_failure.test_name, test_failure.failure_message, f"{test_failure.time:0.2f}s"))
                for test_failure in all_suite_failed.values():
                    if test_failure.key() not in flaky:
                        continue
                    logger.debug(f"Found flaky test: {test_failure}")
                    simple_class_name = test_failure.class_name.split(".")[-1]
                    flaky_table.append((simple_class_name, test_failure.test_name, test_failure.failure_message, f"{test_failure.time:0.2f}s"))
                for skipped_test in suite.skipped_tests:
                    simple_class_name = skipped_test.class_name.split(".")[-1]
                    logger.debug(f"Found skipped test: {skipped_test}")
                    skipped_table.append((simple_class_name, skipped_test.test_name))

                # Only collect quarantined tests from the "quarantinedTest" task
                if task == "quarantinedTest":
                    for test in all_suite_passed.values():
                        simple_class_name = test.class_name.split(".")[-1]
                        quarantined_table.append((simple_class_name, test.test_name))
                    for test in all_suite_failed.values():
                        simple_class_name = test.class_name.split(".")[-1]
                        quarantined_table.append((simple_class_name, test.test_name))

                if args.export_test_catalog:
                    exporter.handle_suite(module_path, suite)

    logger.debug("::endgroup::")

    if args.export_test_catalog:
        logger.debug(f"::group::Generating Test Catalog Files")
        exporter.export(args.export_test_catalog)
        logger.debug("::endgroup::")

    duration = pretty_time_duration(total_time)
    logger.info(f"Finished processing {len(reports)} reports")

    # Print summary of the tests.
    # The stdout (print) goes to the workflow step console output.
    # The stderr (logger) is redirected to GITHUB_STEP_SUMMARY which becomes part of the HTML job summary.
    report_url = get_env("JUNIT_REPORT_URL")
    if report_url:
        report_md = f"Download [HTML report]({report_url})."
    else:
        report_md = "No report available. JUNIT_REPORT_URL was missing."
    summary = (f"{total_run} tests cases run in {duration}.\n\n"
               f"{total_success} {PASSED}, {total_failures} {FAILED}, "
               f"{total_flaky} {FLAKY}, {total_skipped} {SKIPPED}, {len(quarantined_table)} {QUARANTINED}, and {total_errors} errors.")
    print("## Test Summary\n")
    print(f"{summary}\n\n{report_md}\n")

    # Failed
    if len(failed_table) > 0:
        print("<details open=\"true\">")
        print(f"<summary>Failed Tests {FAILED} ({len(failed_table)})</summary>\n")
        print(f"| Module | Test | Message | Time |")
        print(f"| ------ | ---- | ------- | ---- |")
        logger.info(f"Found {len(failed_table)} test failures:")
        for row in failed_table:
            logger.info(f"{FAILED} {row[0]} > {row[1]}")
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
        print("\n</details>")
    print("\n")

    # Flaky
    if len(flaky_table) > 0:
        print("<details open=\"true\">")
        print(f"<summary>Flaky Tests {FLAKY} ({len(flaky_table)})</summary>\n")
        print(f"| Module | Test | Message | Time |")
        print(f"| ------ | ---- | ------- | ---- |")
        logger.info(f"Found {len(flaky_table)} flaky test failures:")
        for row in flaky_table:
            logger.info(f"{FLAKY} {row[0]} > {row[1]}")
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
        print("\n</details>")
    print("\n")

    # Skipped
    if len(skipped_table) > 0:
        print("<details>")
        print(f"<summary>Skipped Tests {SKIPPED} ({len(skipped_table)})</summary>\n")
        print(f"| Module | Test |")
        print(f"| ------ | ---- |")
        logger.debug(f"::group::Found {len(skipped_table)} skipped tests")
        for row in skipped_table:
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
            logger.debug(f"{row[0]} > {row[1]}")
        print("\n</details>")
        logger.debug("::endgroup::")
    print("\n")

    # Quarantined
    if len(quarantined_table) > 0:
        print("<details>")
        print(f"<summary>Quarantined Tests {QUARANTINED} ({len(quarantined_table)})</summary>\n")
        print(f"| Module | Test |")
        print(f"| ------ | ---- |")
        logger.debug(f"::group::Found {len(quarantined_table)} quarantined tests")
        for row in quarantined_table:
            row_joined = " | ".join(row)
            print(f"| {row_joined} |")
            logger.debug(f"{row[0]} > {row[1]}")
        print("\n</details>")
        logger.debug("::endgroup::")

    print("<hr/>")

    # Print special message if there was a timeout
    test_exit_code = get_env("GRADLE_TEST_EXIT_CODE", int)
    quarantined_test_exit_code = get_env("GRADLE_QUARANTINED_TEST_EXIT_CODE", int)

    if test_exit_code == 124 or quarantined_test_exit_code == 124:
        # Special handling for timeouts. The exit code 124 is emitted by 'timeout' command used in build.yml.
        # A watchdog script "thread-dump.sh" will use jstack to force a thread dump for any Gradle process
        # still running after the timeout. We capture the exit codes of the two test tasks and pass them to
        # this script. If either "test" or "quarantinedTest" fails due to timeout, we want to fail the overall build.
        thread_dump_url = get_env("THREAD_DUMP_URL")
        if test_exit_code == 124:
            logger.debug(f"Gradle task for 'test' timed out. These are partial results!")
        else:
            logger.debug(f"Gradle task for 'quarantinedTest' timed out. These are partial results!")
        logger.debug(summary)
        if thread_dump_url:
            print(f"\nThe JUnit tests were cancelled due to a timeout. Thread dumps were generated before the job was cancelled. "
                  f"Download [thread dumps]({thread_dump_url}).\n")
            logger.debug(f"Failing this step because the tests timed out. Thread dumps were taken and archived here: {thread_dump_url}")
        else:
            logger.debug(f"Failing this step because the tests timed out. Thread dumps were not archived, check logs in JUnit step.")
        exit(1)
    elif test_exit_code in (0, 1):
        logger.debug(summary)
        if total_failures > 0:
            logger.debug(f"Failing this step due to {total_failures} test failures")
            exit(1)
        elif total_errors > 0:
            logger.debug(f"Failing this step due to {total_errors} test errors")
            exit(1)
        else:
            exit(0)
    else:
        logger.debug(f"Gradle had unexpected exit code {test_exit_code}. Failing this step")
        exit(1)
