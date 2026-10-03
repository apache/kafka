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

import contextlib
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import runpy
import subprocess
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "pr-format.py"
spec = importlib.util.spec_from_file_location("pr_format", SCRIPT)
pr_format = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pr_format)


class FormatBodyTest(unittest.TestCase):
    def assert_preserved(self, body):
        self.assertEqual(body, pr_format.format_body(body))
        self.assertEqual(body, pr_format.format_body(pr_format.format_body(body)))

    def test_indented_list_from_issue(self):
        self.assert_preserved("Example list:\n - first item\n - second item")

    def test_fence_with_blank_lines_from_issue(self):
        self.assert_preserved("```\nfirst command\n\nsecond command\nthird command\n\n```")

    def test_lists_and_continuations(self):
        for marker in ("-", "*", "+", "1.", "1)"):
            for indent in ("", " ", "  ", "   "):
                with self.subTest(marker=marker, indent=indent):
                    self.assert_preserved(
                        f"{indent}{marker} parent item {'long text ' * 15}\n"
                        f"{indent}    continuation\n\n"
                        f"{indent}    another paragraph\n\n"
                        f"{indent}    - nested child\n"
                        f"{indent}{marker} second item\n"
                    )

    def test_fences_and_indented_code(self):
        command = "    call(" + ", ".join(["argument"] * 15) + ")  \n"
        for body in (
            "```python\n" + command + "\n\nnext command\n```\n",
            "~~~sh\n" + command + "\nnext command\n~~~\n",
            "````\n```\n" + command + "```\n\n````\n",
            "```\n" + command + "\n~~~\nnot closed\n",
            "   ```\n" + command + "\n   ```\n",
            command + "\n    next command\n",
            "\tfirst command\n\n\tsecond command\n",
            "> ```\n> first\n>\n> second\n> ```\n",
            "- code:\n\n  ```\n  first\n\n  second\n  ```\n",
        ):
            with self.subTest(body=body):
                self.assert_preserved(body)

    def test_other_markdown(self):
        for body in (
            "> " + "quoted text " * 20 + "\n> another line\n",
            "# " + "heading " * 20 + "\n",
            "Setext heading\n==============\n",
            "---\n",
            "| Name | Value |\n| --- | --- |\n| A | " + "value " * 20 + "|\n",
            "<details>\n\n<summary>Example</summary>\n\n</details>\n",
            "[reference]: https://example.org/" + "path/" * 20 + "\n",
            "Paragraph " + "text " * 20 + "`inline code`\n",
            "Paragraph " + "text " * 20 + "[a link](https://example.org)\n",
            "Paragraph " + "text " * 20 + "**emphasis**\n",
            "Paragraph " + "text " * 20 + "~~strikethrough~~\n",
            "Paragraph " + "text " * 20 + "  \nhard break\n",
            "Paragraph " + "text " * 20 + "\\\nhard break\n",
            "   indented prose " + "text " * 20 + "\n",
            "Reviewers: " + "text " * 20 + "`inline code`\n",
        ):
            with self.subTest(body=body):
                self.assert_preserved(body)

    def test_wrap_prose(self):
        body = "Plain prose words " * 15
        formatted = pr_format.format_body(body)
        self.assertNotEqual(body, formatted)
        self.assertEqual(body.split(), formatted.split())
        self.assertTrue(all(len(line) <= 72 for line in formatted.splitlines()))
        self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_mixed_prose_and_markdown(self):
        prose = "Plain prose words " * 15 + "\n"
        markdown = " - first item\n - second item\n\n```\n    code\n\n    more code\n```\n"
        body = prose + "\n" + markdown + "\n" + prose
        expected = pr_format.format_body(prose) + "\n" + markdown + "\n" + pr_format.format_body(prose)
        self.assertEqual(expected, pr_format.format_body(body))
        self.assertEqual(expected, pr_format.format_body(expected))

    def test_preserve_separators_and_line_endings(self):
        for newline in ("\n", "\r\n", "\r"):
            for ending in ("", newline):
                with self.subTest(newline=newline, ending=ending):
                    body = newline * 2 + "short prose" + newline * 3 + "```" + newline + "code" + newline * 2 + "```" + ending
                    self.assert_preserved(body)
                    prose = "words" + newline + "words " * 25 + ending
                    formatted = pr_format.format_body(prose)
                    self.assertEqual(formatted, pr_format.format_body(formatted))
                    self.assertEqual(bool(ending), formatted.endswith(newline))
                    self.assertNotIn("\n", formatted.replace(newline, ""))
        for body in ("", "\n\n", "short paragraph", "short paragraph\n"):
            self.assert_preserved(body)

    def test_unicode_separator_does_not_shift_code_source_map(self):
        self.assert_preserved("hello\u2028world\n\n```\n    code\n\n    more code\n```\n")

    def test_long_words_and_hyphens_are_not_broken(self):
        for word in ("https://example.org/" + "path/" * 30, "hyphen-" * 30):
            with self.subTest(word=word):
                formatted = pr_format.format_body("See " + word + " for details.\n")
                self.assertIn(word, formatted)
                self.assertEqual(formatted, pr_format.format_body(formatted))

    def test_wrapping_does_not_introduce_markdown_blocks(self):
        prefix = "word " * 14 + "ab "
        for marker in ("- item", "> quote", "# heading", "1. item", "```", "---"):
            with self.subTest(marker=marker):
                self.assert_preserved(prefix + marker + "\n")

    def test_wrapping_does_not_introduce_hard_breaks(self):
        self.assert_preserved("word " * 14 + "\\ more prose\n")

    def test_reviewers_trailer_remains_parseable(self):
        reviewers = "Alice Example <alice@example.org>, Bob Example <bob@example.org>, Carol Example <carol@example.org>"
        body = "Description.\n\nReviewers: " + reviewers + "\n"
        formatted = pr_format.format_body(body)
        self.assertNotEqual(body, formatted)
        self.assertEqual([reviewers], pr_format.parse_trailers("KAFKA-21205 Preserve Markdown", formatted)["Reviewers"])
        self.assertEqual(formatted, pr_format.format_body(formatted))


class ScriptTest(unittest.TestCase):
    def run_script(self, body, actions=True, reviews=None):
        edits = []
        real_run = subprocess.run

        def fake_run(command, **kwargs):
            if command[:3] == ["gh", "pr", "view"]:
                data = {"title": "KAFKA-21205 Preserve Markdown formatting", "body": body, "reviews": reviews or []}
                return subprocess.CompletedProcess(command, 0, json.dumps(data).encode(), b"")
            if command[:3] == ["gh", "pr", "edit"]:
                edits.append(Path(command[command.index("--body-file") + 1]).read_bytes().decode())
                return subprocess.CompletedProcess(command, 0, b"", b"")
            if command[:2] == ["git", "interpret-trailers"]:
                return real_run(command, **kwargs)
            self.fail(f"Unexpected subprocess: {command}")

        logger = logging.getLogger("pr-format")
        old_handlers = logger.handlers[:]
        old_level = logger.level
        logger.handlers.clear()
        environment = {"PR_NUMBER": "21205"}
        if actions:
            environment["GITHUB_ACTIONS"] = "true"
        try:
            with patch.dict(os.environ, environment, clear=True), patch("subprocess.run", side_effect=fake_run), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as exit_info:
                    runpy.run_path(str(SCRIPT), run_name="__main__")
                return exit_info.exception.code, edits
        finally:
            logger.handlers[:] = old_handlers
            logger.setLevel(old_level)

    def test_unchanged_markdown_is_not_written(self):
        for body in ("Example list:\n - first item\n - second item", "```\nfirst\n\nsecond\nthird\n\n```", "short prose\n"):
            with self.subTest(body=body):
                self.assertEqual((0, []), self.run_script(body))

    def test_changed_body_is_written_once_and_then_skipped(self):
        body = "Plain prose words " * 15
        status, edits = self.run_script(body)
        self.assertEqual(0, status)
        self.assertEqual([pr_format.format_body(body)], edits)
        self.assertEqual((0, []), self.run_script(edits[0]))

    def test_local_run_never_writes(self):
        self.assertEqual((0, []), self.run_script("Plain prose words " * 15, actions=False))

    def test_approved_pr_still_requires_reviewers(self):
        reviews = [{"authorAssociation": "MEMBER", "state": "APPROVED"}]
        self.assertEqual((1, []), self.run_script("Description.\n", reviews=reviews))
        self.assertEqual((0, []), self.run_script("Description.\n\nReviewers: Alice <alice@example.org>\n", reviews=reviews))


if __name__ == "__main__":
    unittest.main()
