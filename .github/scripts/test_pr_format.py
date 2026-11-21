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
import io
import itertools
import json
import runpy
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import call, patch


SCRIPT = Path(__file__).with_name("pr-format.py")
SPEC = importlib.util.spec_from_file_location("pr_format", SCRIPT)
pr_format = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pr_format)


class FormatBodyTest(unittest.TestCase):
    def test_wraps_plain_prose_at_72_columns(self):
        body = (
            "This is a plain   paragraph with enough words to wrap at the expected "
            "column while keeping every word and the paragraph meaning intact.\n"
        )

        formatted = pr_format.format_body(body)

        self.assertEqual(
            "This is a plain paragraph with enough words to wrap at the expected\n"
            "column while keeping every word and the paragraph meaning intact.\n",
            formatted,
        )
        self.assertTrue(all(len(line) <= 72 for line in formatted.splitlines()))
        self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_preserves_list_markers(self):
        prefixes = [
            "".join(p) for p in itertools.product(("", " "), ("-", "+", "*"), ("", " "))
        ]
        for prefix in prefixes + ["1. ", "1) "]:
            with self.subTest(prefix=prefix):
                body = f"Example list:\n{prefix}first item\n{prefix}second item\n"
                formatted = pr_format.format_body(body)
                self.assertEqual(body, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_preserves_fenced_code_and_long_indented_lines(self):
        for marker, other_marker in (("`", "~"), ("~", "`")):
            with self.subTest(marker=marker):
                code = (
                    f"{marker * 4}text\n"
                    "first code line\n\n"
                    "    " + ("long code content " * 8) + "\n"
                    f"{other_marker * 3}\n"
                    "second code line\nthird code line\n"
                    f"{marker * 3}\n"
                    "fourth code line\nfifth code line\n"
                    f"{marker * 5}\n"
                )
                body = "Prose before\nthe code fence.\n" + code + "Prose after\nthe code fence.\n"
                expected = "Prose before the code fence.\n" + code + "Prose after the code fence.\n"
                formatted = pr_format.format_body(body)
                self.assertEqual(expected, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_preserves_structured_content_and_hard_breaks(self):
        for body in (
            "# Heading\n\n## Another heading\n",
            "> A quotation\n> on separate lines\n",
            "    " + "long indented code " * 8 + "\n\n\tmore code\n",
            "| Name  | Value |\n|-------|-------|\n| alpha | beta  |\n",
            "Text\n___\n\n***\n\n---\n\n===\n",
            "<details>\n<summary>Details</summary>\n</details>\n",
            '[reference]: https://example.com "A reference title long enough '
            'that prose wrapping would split this definition."\n',
            "A deliberate hard break  \nfollowed by another line.\n",
            "A deliberate hard break\\\nfollowed by another line.\n",
            "```text\nunclosed fence\n\nmore code\n",
            "- parent\n  - nested child\n",
        ):
            with self.subTest(body=body):
                formatted = pr_format.format_body(body)
                self.assertEqual(body, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_wraps_inline_prose(self):
        cases = (
            (
                "This change makes `GroupCoordinatorService` validate the "
                "`group.consumer.session.timeout.ms` config before the group is loaded.\n",
                "This change makes `GroupCoordinatorService` validate the\n"
                "`group.consumer.session.timeout.ms` config before the group is loaded.\n",
            ),
            (
                "This change makes group_consumer_session_timeout validate the session "
                "configuration before the group is loaded.\n",
                "This change makes group_consumer_session_timeout validate the session\n"
                "configuration before the group is loaded.\n",
            ),
            (
                "This change follows [the proposed configuration validation "
                "behavior](https://example.com/design) before the group is loaded.\n",
                "This change follows [the proposed configuration validation\n"
                "behavior](https://example.com/design) before the group is loaded.\n",
            ),
        )
        for body, expected in cases:
            with self.subTest(body=body):
                formatted = pr_format.format_body(body)
                self.assertEqual(expected, formatted)
                self.assertTrue(all(len(line) <= 72 for line in formatted.splitlines()))
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_unfenced_benchmark_output_is_stable(self):
        body = (
            "Benchmark                                                        Mode  Cnt      Score      Error   Units\n"
            "ProducerMetadataAddBenchmark.addExistingTopicFullySynchronized  thrpt    5   6217.287 ± 1279.054  ops/ms\n"
        )
        formatted = pr_format.format_body(body)
        self.assertEqual(body.split(), formatted.split())
        self.assertNotIn("  ", formatted)
        self.assertTrue(all(len(line) <= 72 for line in formatted.splitlines()))
        self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_reviewer_trailers_and_continuations(self):
        reviewers = (
            "Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper "
            "<grace@example.com>, Katherine Johnson <katherine@example.com>\n"
        )
        wrapped_reviewers = (
            "Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper\n"
            " <grace@example.com>, Katherine Johnson <katherine@example.com>\n"
        )
        other = "Generated-by: Codex (GPT-6)\n"
        for body, expected, parsed in (
            (reviewers, wrapped_reviewers, reviewers),
            (reviewers + other, wrapped_reviewers + other, reviewers + other),
            (other + reviewers, other + wrapped_reviewers, other + reviewers),
        ):
            with self.subTest(body=body):
                formatted = pr_format.format_body(body)
                self.assertEqual(expected, formatted)
                result = subprocess.run(
                    ["git", "interpret-trailers", "--trim-empty", "--parse"],
                    input=("KAFKA-21205 preserve Markdown\n\n" + formatted).encode(),
                    capture_output=True,
                    check=True,
                )
                self.assertEqual(parsed.encode(), result.stdout)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_preserves_other_trailers_and_continuations(self):
        body = (
            "Generated-by:   Codex (GPT-6)\n"
            " with an indented continuation\n"
            "Reviewers:   Ada Lovelace <ada@example.com>\n"
            "Signed-off-by:   Example Person <example@example.com>\n"
        )
        formatted = pr_format.format_body(body)
        self.assertEqual(body.replace("Reviewers:   ", "Reviewers: "), formatted)
        self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_normalizes_line_endings(self):
        expected = "Short prose.\n\n```\nfirst line\n\nsecond line\n```\n"
        for body in (
            expected.replace("\n", "\r\n"),
            expected.replace("\n", "\r"),
            "Short prose.\r\n\n```\r\nfirst line\n\r\nsecond line\r```\n",
        ):
            with self.subTest(body=body):
                formatted = pr_format.format_body(body)
                self.assertEqual(expected, formatted)
                self.assertNotIn("\r", formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_empty_input_and_final_newline(self):
        for body in ("", "\n", "Short prose.", "Short prose.\n", "Short prose.\n\n"):
            with self.subTest(body=body):
                formatted = pr_format.format_body(body)
                self.assertEqual(body, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_does_not_create_block_or_trailer_boundaries(self):
        for suffix in ("-item", "Reviewers: Example Person", "~~~", "### Heading"):
            body = "A " * 35 + suffix + " and some more prose after the boundary.\n"
            with self.subTest(suffix=suffix):
                formatted = pr_format.format_body(body)
                self.assertEqual(body, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_long_unbroken_words(self):
        identifier = "long-" * 20 + "name"
        body = f"An identifier {identifier} stays intact.\n"
        formatted = pr_format.format_body(body)
        self.assertEqual(f"An identifier\n{identifier}\nstays intact.\n", formatted)
        self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_cli_updates_only_changed_bodies(self):
        for body, expected in (
            ("Short prose.\n", "Short prose.\n"),
            ("Short prose.\r\n", "Short prose.\n"),
            ("First prose line\nsecond prose line.\n", "First prose line second prose line.\n"),
        ):
            with self.subTest(body=body):
                response = subprocess.CompletedProcess(
                    args=[], returncode=0,
                    stdout=json.dumps({
                        "title": "KAFKA-21205 Preserve Markdown",
                        "body": body, "reviews": [],
                    }).encode(),
                    stderr=b"",
                )
                with (
                    patch.dict("os.environ", {"PR_NUMBER": "23691", "GITHUB_ACTIONS": "true"}, clear=True),
                    patch("subprocess.run", return_value=response) as run,
                    patch("tempfile.NamedTemporaryFile") as temporary_file,
                    patch("builtins.exit", new=sys.exit, create=True),
                    patch("logging.getLogger"),
                    patch("sys.stdout", new_callable=io.StringIO),
                ):
                    body_file = temporary_file.return_value.__enter__.return_value
                    body_file.name = "pr-body.txt"
                    with self.assertRaises(SystemExit) as result:
                        runpy.run_path(str(SCRIPT), run_name="__main__")
                    self.assertEqual(0, result.exception.code)
                    expected_calls = [call(
                        ["gh", "pr", "view", "23691", "--json", "title,body,reviews"],
                        capture_output=True,
                    )]
                    if body != expected:
                        expected_calls.append(call(
                            ["gh", "pr", "edit", "23691", "--body-file", body_file.name],
                            capture_output=True,
                        ))
                        temporary_file.assert_called_once_with()
                        body_file.write.assert_called_once_with(expected.encode())
                    else:
                        temporary_file.assert_not_called()
                    self.assertEqual(expected_calls, run.call_args_list)


if __name__ == "__main__":
    unittest.main()
