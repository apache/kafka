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

import importlib.util
import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("pr-format.py")
SPEC = importlib.util.spec_from_file_location("pr_format", SCRIPT)
pr_format = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pr_format)


class FormatBodyTest(unittest.TestCase):
    def test_wraps_plain_prose_at_72_columns(self):
        body = (
            "This is a plain paragraph with enough words to wrap at the expected "
            "column while keeping every word and the paragraph meaning intact.\n"
        )

        formatted = pr_format.format_body(body)

        self.assertEqual(
            "This is a plain paragraph with enough words to wrap at the expected\n"
            "column while keeping every word and the paragraph meaning intact.\n",
            formatted,
        )
        self.assertTrue(all(len(line) <= 72 for line in formatted.splitlines()))

    def test_preserves_indented_list_reproduction(self):
        body = "Example list:\n - first item\n - second item\n"

        self.assertEqual(body, pr_format.format_body(body))

    def test_preserves_fenced_code_and_long_indented_lines(self):
        body = (
            "```text\n"
            "first code line\n"
            "\n"
            "    " + ("long code content " * 8) + "\n"
            "last code line\n"
            "```\n"
        )

        self.assertEqual(body, pr_format.format_body(body))

    def test_preserves_inline_markdown_and_aligned_tables(self):
        body = (
            "This paragraph has `inline code` and [a link](https://example.com).\n\n"
            "| Name  | Value |\n"
            "|-------|-------|\n"
            "| alpha | beta  |\n"
        )

        self.assertEqual(body, pr_format.format_body(body))

    def test_reviewer_continuations_remain_parseable(self):
        body = (
            "Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper "
            "<grace@example.com>, Katherine Johnson <katherine@example.com>\n"
        )

        formatted = pr_format.format_body(body)

        self.assertIn("\n ", formatted)
        result = subprocess.run(
            ["git", "interpret-trailers", "--trim-empty", "--parse"],
            input=("KAFKA-21205 preserve Markdown\n\n" + formatted).encode(),
            capture_output=True,
            check=True,
        )
        self.assertEqual(
            b"Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper <grace@example.com>, "
            b"Katherine Johnson <katherine@example.com>\n",
            result.stdout,
        )

    def test_formatting_is_idempotent(self):
        body = (
            "Plain prose that is long enough to wrap across multiple lines "
            "without changing its words or meaning.\n\n"
            "```text\n"
            "code stays exactly as written\n"
            "\n"
            "    indented code stays intact\n"
            "```\n"
        )

        formatted = pr_format.format_body(body)

        self.assertEqual(formatted, pr_format.format_body(formatted))


if __name__ == "__main__":
    unittest.main()
