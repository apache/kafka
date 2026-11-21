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

# Tests for pr-format.py and pr-reviewers-trailer.py. Both scripts are run as
# subprocesses against a fake `gh` executable, so no network access and no
# GitHub token are needed. Run with:
#
#   python -m unittest discover -s .github/scripts -p 'test_pr_scripts.py'

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = os.path.dirname(os.path.abspath(__file__))

# Records every call. Serves the PR from pr.json, saves an edited body to
# edited-body.txt, and resolves any reviewer login to "New Person".
FAKE_GH = '''#!{python}
import json, os, sys
args = sys.argv[1:]
state = os.environ["FAKE_GH_DIR"]
with open(os.path.join(state, "calls.log"), "a") as f:
    f.write(json.dumps(args) + "\\n")
if args[:2] == ["pr", "view"]:
    with open(os.path.join(state, "pr.json")) as f:
        sys.stdout.write(f.read())
elif args[:2] == ["pr", "edit"]:
    with open(args[args.index("--body-file") + 1], "rb") as src:
        with open(os.path.join(state, "edited-body.txt"), "wb") as dst:
            dst.write(src.read())
elif args[0] == "api" and "commits?author=" in args[1]:
    print(json.dumps([{{"commit": {{"author": {{"name": "New Person", "email": "new@example.com"}}}}}}]))
else:
    print("[]")
'''

TITLE = "KAFKA-12345: A title that passes the linter"


class FakeGh:
    def __init__(self, body, reviews=None):
        self.dir = tempfile.mkdtemp()
        gh = os.path.join(self.dir, "gh")
        with open(gh, "w") as f:
            f.write(FAKE_GH.format(python=sys.executable))
        os.chmod(gh, os.stat(gh).st_mode | stat.S_IEXEC)
        pr = {"title": TITLE, "body": body, "author": {"login": "author"}, "reviews": reviews or []}
        with open(os.path.join(self.dir, "pr.json"), "w") as f:
            json.dump(pr, f)

    def run(self, script, **env):
        full_env = dict(os.environ, FAKE_GH_DIR=self.dir, GITHUB_ACTIONS="true", PR_NUMBER="1", **env)
        full_env["PATH"] = self.dir + os.pathsep + full_env["PATH"]
        return subprocess.run([sys.executable, os.path.join(SCRIPTS, script)],
                              env=full_env, capture_output=True, text=True)

    def calls(self):
        with open(os.path.join(self.dir, "calls.log")) as f:
            return [json.loads(line) for line in f]

    def edited_body(self):
        path = os.path.join(self.dir, "edited-body.txt")
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read().decode()


def add_reviewer(body):
    gh = FakeGh(body)
    result = gh.run("pr-reviewers-trailer.py", REVIEWER_LOGIN="newperson")
    assert result.returncode == 0, result.stderr
    return gh


def parsed_trailers(body):
    message = (TITLE + "\n\n" + body).encode()
    out = subprocess.run(["git", "interpret-trailers", "--parse"], input=message, capture_output=True)
    return out.stdout.decode().splitlines()


# What a contributor may write. The tools must leave all of it alone.
AUTHOR_TEXT = (
    "This paragraph has a line that is much longer than seventy-two characters and it must stay on one line.\n"
    "\n"
    "| mode | before | after |\n"
    "|---|---|---|\n"
    "| classic consumer with a long name | 123456 records/sec | 234567 records/sec |\n"
    "\n"
    "Example list:\n"
    " - first item\n"
    " - second item\n"
    "\n"
    "```\n"
    "first command\n"
    "\n"
    "second command\n"
    "    third command, indented\n"
    "```\n"
    "\n"
    "Line with a hard break  \n"
    "next line"
)


class PrFormatTest(unittest.TestCase):

    def test_linter_never_edits_the_description(self):
        gh = FakeGh(AUTHOR_TEXT + "\n")
        result = gh.run("pr-format.py")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("PR format looks good!", result.stdout.strip())
        self.assertEqual([], [c for c in gh.calls() if c[:2] == ["pr", "edit"]])

    def test_linter_still_reports_a_bad_title(self):
        gh = FakeGh("Some body\n")
        with open(os.path.join(gh.dir, "pr.json"), "w") as f:
            json.dump({"title": "No prefix on this title", "body": "Some body\n", "reviews": []}, f)
        result = gh.run("pr-format.py")
        self.assertEqual(1, result.returncode)
        self.assertIn("KAFKA-XXXXX or MINOR/HOTFIX", result.stdout)

    def test_linter_requires_reviewers_once_approved(self):
        approved = [{"authorAssociation": "MEMBER", "state": "APPROVED"}]
        missing = FakeGh("Some body\n", approved).run("pr-format.py")
        self.assertEqual(1, missing.returncode)
        self.assertIn("no 'Reviewers' found", missing.stdout)

        present = FakeGh("Some body\n\nReviewers: A <a@example.com>\n", approved).run("pr-format.py")
        self.assertEqual(0, present.returncode, present.stderr)

    def test_linter_reports_text_below_the_reviewers_trailer(self):
        message = "'Reviewers:' must be in the last paragraph. Move the text below it above it."
        for below in ("\nPlease backport to 4.2.\n",           # text after a blank line
                      "Please backport to 4.2.\n",             # text on the next line
                      "\nCo-Authored-By: Bot <b@example.com>\n"):  # trailer after a blank line
            result = FakeGh("Some body\n\nReviewers: A <a@example.com>\n" + below).run("pr-format.py")
            self.assertEqual(1, result.returncode, below)
            self.assertIn(message, result.stdout)

        next_line = "Some body\n\nReviewers: A <a@example.com>\nCo-Authored-By: Bot <b@example.com>\n"
        self.assertEqual(0, FakeGh(next_line).run("pr-format.py").returncode)


class ReviewersTrailerTest(unittest.TestCase):

    def test_author_text_is_kept_byte_for_byte(self):
        edited = add_reviewer(AUTHOR_TEXT + "\n").edited_body()
        self.assertEqual(AUTHOR_TEXT + "\n\nReviewers: New Person <new@example.com>\n", edited)

    def test_crlf_author_text_is_kept_byte_for_byte(self):
        crlf = AUTHOR_TEXT.replace("\n", "\r\n")
        edited = add_reviewer(crlf + "\r\n").edited_body()
        self.assertTrue(edited.startswith(crlf), edited)
        self.assertEqual(["Reviewers: New Person <new@example.com>"], parsed_trailers(edited))

    def test_adjacent_trailers_stay_on_separate_lines(self):
        # KAFKA-21579
        body = ("Fix the thing.\n\n"
                "Co-Authored-By: Some Bot <noreply@example.com>\n"
                "Reviewers: Ken Huang <ken@example.com>, Federico Valeri <fede@example.com>\n")
        edited = add_reviewer(body).edited_body()
        self.assertEqual(
            "Fix the thing.\n\n"
            "Co-Authored-By: Some Bot <noreply@example.com>\n"
            "Reviewers: Ken Huang <ken@example.com>,\n"
            " Federico Valeri <fede@example.com>, New Person <new@example.com>\n",
            edited)
        self.assertEqual(
            ["Co-Authored-By: Some Bot <noreply@example.com>",
             "Reviewers: Ken Huang <ken@example.com>, Federico Valeri <fede@example.com>, New Person <new@example.com>"],
            parsed_trailers(edited))

    def test_trailer_after_reviewers_is_kept(self):
        body = ("Fix the thing.\n\n"
                "Reviewers: Ada Lovelace <ada@example.com>\n"
                "Generated-by: Some Tool (v1)\n")
        edited = add_reviewer(body).edited_body()
        self.assertEqual(
            ["Generated-by: Some Tool (v1)",
             "Reviewers: Ada Lovelace <ada@example.com>, New Person <new@example.com>"],
            parsed_trailers(edited))
        self.assertTrue(edited.startswith("Fix the thing.\n\n"), edited)

    def test_long_reviewers_trailer_is_folded_within_72_columns(self):
        body = ("Fix the thing.\n\n"
                "Reviewers: Ada Lovelace <ada@example.com>, Alan Turing <alan@example.com>,\n"
                " Grace Hopper <grace@example.com>\n")
        edited = add_reviewer(body).edited_body()
        trailer_lines = edited.split("\n\n")[-1].splitlines()
        self.assertTrue(all(len(line) <= 72 for line in trailer_lines), trailer_lines)
        self.assertTrue(all(line.startswith(" ") for line in trailer_lines[1:]), trailer_lines)
        self.assertEqual(
            ["Reviewers: Ada Lovelace <ada@example.com>, Alan Turing <alan@example.com>, "
             "Grace Hopper <grace@example.com>, New Person <new@example.com>"],
            parsed_trailers(edited))

    def test_a_reviewer_name_stays_next_to_its_email(self):
        body = ("Fix the thing.\n\n"
                "Reviewers: Alice <alice@example.com>, Bob <bob@example.com>, Carol <carol@example.com>\n")
        edited = add_reviewer(body).edited_body()
        self.assertEqual(
            "Fix the thing.\n\n"
            "Reviewers: Alice <alice@example.com>, Bob <bob@example.com>,\n"
            " Carol <carol@example.com>, New Person <new@example.com>\n",
            edited)

    def test_one_reviewer_longer_than_72_columns_is_still_folded(self):
        long_one = "A Reviewer With A Very Long Display Name Indeed <a.very.long.address@example.com>"
        edited = add_reviewer("Fix the thing.\n\nReviewers: " + long_one + "\n").edited_body()
        trailer_lines = edited.split("\n\n")[-1].splitlines()
        self.assertTrue(all(len(line) <= 72 for line in trailer_lines), trailer_lines)
        self.assertEqual(["Reviewers: " + long_one + ", New Person <new@example.com>"], parsed_trailers(edited))

    def test_leading_indentation_is_kept(self):
        edited = add_reviewer("    indented first line\n\nmore text\n").edited_body()
        self.assertEqual("    indented first line\n\nmore text\n\nReviewers: New Person <new@example.com>\n", edited)

    def test_body_with_only_a_reviewers_trailer_gets_no_second_trailer(self):
        edited = add_reviewer("Reviewers: A <a@example.com>\n").edited_body()
        self.assertEqual("Reviewers: A <a@example.com>, New Person <new@example.com>\n", edited)

    def test_hand_written_long_trailer_is_folded_even_if_reviewer_is_present(self):
        body = ("Fix the thing.\n\n"
                "Reviewers: New Person <new@example.com>, Ada Lovelace <ada@example.com>, Alan Turing <alan@example.com>\n")
        edited = add_reviewer(body).edited_body()
        self.assertEqual(
            "Fix the thing.\n\n"
            "Reviewers: New Person <new@example.com>, Ada Lovelace <ada@example.com>,\n"
            " Alan Turing <alan@example.com>\n",
            edited)

    def test_no_second_trailer_when_text_is_below_reviewers(self):
        body = "Fix the thing.\n\nReviewers: A <a@example.com>\n\nPlease backport to 4.2.\n"
        self.assertIsNone(add_reviewer(body).edited_body())

    def test_adding_the_same_reviewer_twice_changes_nothing(self):
        first = add_reviewer("Fix the thing.\n").edited_body()
        self.assertIsNone(add_reviewer(first).edited_body())

    def test_description_is_read_after_the_reviewer_is_resolved(self):
        # Resolving a reviewer takes several API calls. Reading the description
        # last keeps the window in which a concurrent edit can be lost small.
        calls = add_reviewer("Fix the thing.\n").calls()
        last_api = max(i for i, c in enumerate(calls) if c[0] == "api")
        body_reads = [i for i, c in enumerate(calls) if c[:2] == ["pr", "view"] and "body" in c[-1]]
        self.assertEqual(1, len(body_reads))
        self.assertGreater(body_reads[0], last_api)

    def test_pr_author_is_not_added(self):
        gh = FakeGh("Fix the thing.\n")
        result = gh.run("pr-reviewers-trailer.py", REVIEWER_LOGIN="author")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIsNone(gh.edited_body())


if __name__ == "__main__":
    unittest.main()
