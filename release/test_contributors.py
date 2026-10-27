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
Unit tests for the contributors module.
Run with: python -m pytest release/test_contributors.py -v
   or:    cd release && python -m unittest test_contributors -v
"""

import os
import shutil
import subprocess
import tempfile
import unittest

import contributors
import templates


class ParsePeopleTest(unittest.TestCase):

    def test_split_people(self):
        self.assertEqual(
            ['A <a@x>', '"B, C" <b@x>', 'D', 'E<e@x>', 'F <f@x>', 'G <g@x'],
            contributors.split_people('A <a@x>, "B, C" <b@x>, D, E<e@x> F <f@x>, G <g@x'))

    def test_parse_person(self):
        parse = contributors.parse_person
        self.assertEqual(("Ann Lee", "ann@x", None), parse("Ann Lee <ann@x>"))
        self.assertEqual(("Ann Lee", "ann@x", None), parse("Ann Lee <ann@x"))
        self.assertEqual(("Ann Lee", "ann@x", None), parse("Ann Lee [ann@x](mailto:ann@x)"))
        self.assertEqual(("Gyeongwon, Do", "dct@x", None), parse('"Gyeongwon, Do" <dct@x>'))
        self.assertEqual(('TingIāu "Ting" Kì', "ting@x", None), parse('TingIāu "Ting" Kì <ting@x>'))
        self.assertEqual(("Ann Lee", None, "annlee"), parse("Ann Lee (github:annlee)"))
        self.assertEqual(("Ann Lee", None, "annlee"), parse("Ann Lee (@annlee)"))
        self.assertEqual((None, None, "annlee"), parse("@annlee"))
        self.assertEqual((None, "ann@x", None), parse("ann@x"))
        self.assertEqual(("Ann Lee", None, None), parse("Ann Lee"))
        self.assertEqual(("Claude Opus 4.6 (1M context)", "noreply@anthropic.com", None),
                         parse("Claude Opus 4.6 (1M context) <noreply@anthropic.com>"))


class OfReleaseTest(unittest.TestCase):

    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.mailmap = os.path.join(self.repo, ".git", "contributors.mailmap")
        self._git("init", "-q")
        self._commit("Release Manager <rm@example.com>")
        self._git("tag", "1.0")

    def tearDown(self):
        shutil.rmtree(self.repo)

    def _git(self, *args):
        subprocess.run(["git", "-c", "user.name=Committer", "-c", "user.email=committer@example.com",
                        "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
                       cwd=self.repo, check=True, capture_output=True)

    def _commit(self, author, *trailers):
        message = "Change\n\nDescription of the change.\n\n" + "\n".join(trailers)
        self._git("commit", "-q", "--allow-empty", f"--author={author}", "-m", message)

    def _commit_before_release(self, author, *trailers):
        self._git("tag", "-d", "1.0")
        self._commit(author, *trailers)
        self._git("tag", "1.0")

    def _release(self, *mailmap_lines, report=None):
        with open(self.mailmap, "w") as f:
            f.write("".join(f"{line}\n" for line in mailmap_lines))
        self._git("tag", "2.0")
        return contributors.of_release("1.0", "2.0", mailmap=self.mailmap, cwd=self.repo, report=report)

    def test_credits_authors_co_authors_and_every_reviewer(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-authored-by: Bo Kim <bo@example.com>",
                     "Reviewers: Cy Park <cy@example.com>, Di Wu <di@example.com>, Ed Fox <ed@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu", "Ed Fox"], []), self._release())

    def test_only_credits_commits_of_the_release(self):
        self._commit_before_release("Old Timer <old@example.com>")
        self._commit("Ann Lee <ann@example.com>")
        self.assertEqual((["Ann Lee"], []), self._release())

    def test_groups_names_with_the_same_email_and_prefers_a_real_name(self):
        self._commit("OmniaGM <omnia@example.com>")
        self._commit("OmniaGM <omnia@example.com>")
        self._commit("Omnia Ibrahim <omnia@example.com>")
        self.assertEqual((["Omnia Ibrahim"], []), self._release())

    def test_groups_names_that_only_differ_in_case_accents_and_spacing(self):
        self._commit("Jiayáo Sun <jiayao@example.com>")
        self._commit("Jiayáo Sun <jiayao@example.com>")
        self._commit("jiayao sun <other@example.com>")
        self._commit("Gabriel Dutra <gabriel@example.com>")
        self._commit("gabrieldutra <dutra@example.com>")
        self.assertEqual((["Gabriel Dutra", "Jiayáo Sun"], []), self._release())

    def test_groups_github_logins_with_github_noreply_emails(self):
        self._commit("Uladzislau Blok <123+UladzislauBlok@users.noreply.github.com>",
                     "Reviewers: @UladzislauBlok, Mingi Cho (github:ChoMinGi)")
        self._commit("ChoMinGi <456+ChoMinGi@users.noreply.github.com>")
        self.assertEqual((["Mingi Cho", "Uladzislau Blok"], []), self._release())

    def test_groups_github_logins_with_the_local_part_of_emails(self):
        self._commit("Chia-Ping Tsai <chia7712@example.com>", "Reviewers: @chia7712")
        self._commit("Ann Lee <mail@example.com>")
        self._commit("Bo Kim <mail@example.org>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Chia-Ping Tsai"], []), self._release())

    def test_does_not_group_people_by_placeholder_names(self):
        self._commit("Zhiyan Tang <zhiyan@example.com>")
        self._commit("anonymous <zhiyan@example.com>")
        self._commit("anonymous <someone@example.com>")
        self.assertEqual((["anonymous", "Zhiyan Tang"], []), self._release())

    def test_uses_older_commits_to_fix_names_typed_by_reviewers(self):
        self._commit_before_release("Christo Lolov <clolov@example.com>")
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Chriso Lolov <clolov@example.com>")
        self.assertEqual((["Ann Lee", "Christo Lolov"], []), self._release())

    def test_prefers_the_most_recent_name_when_used_as_much(self):
        self._commit("YuChia Ma <yuchia@example.com>")
        self._commit("Yu-Chia Ma <other@example.com>")
        self._commit("yuchia <yuchia@example.com>")
        self.assertEqual((["Yu-Chia Ma"], []), self._release())

    def test_parses_malformed_reviewers(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewers: Reviewers: Bo Kim [bo@example.com](mailto:bo@example.com), "
                     "Cy Park<cy@example.com> Di Wu <di@example.com>, \"Gyeongwon, Do\" <do@example.com>, "
                     "Shaan, Ed Fox <ed@example.com",
                     "Reviewer: Fay Lin <fay@example.com> and Gus Hill <gus@example.com>, . Hal Ng <hal@example.com>, "
                     "Reviewers: Ivy Ho <ivy@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu", "Ed Fox", "Fay Lin", "Gus Hill", "Gyeongwon Do",
                           "Hal Ng", "Ivy Ho", "Shaan"], []),
                         self._release())

    def test_credits_ais_separately_and_drops_bots(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-authored-by: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
                     "Co-authored-by: Claude Sonnet 4.6 <claude@anthropic.com>",
                     "Co-authored-by: Copilot Autofix powered by AI <175728472+Copilot@users.noreply.github.com>",
                     "Co-authored-by: Cursor <cursoragent@cursor.com>",
                     "Generated-by: Other Tool 1.0")
        self._commit("Bo Kim <bo@example.com>",
                     "Co-authored-by: Claude Opus 4.6 <noreply@anthropic.com>",
                     "Co-authored-by: Copilot <175728472+Copilot@users.noreply.github.com>",
                     "Co-authored-by: Cy Park <cy@anthropic.com>")
        self._commit("dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park"],
                          ["Claude Opus 4.6", "Claude Sonnet 4.6", "Copilot", "Cursor", "Other Tool 1.0"]),
                         self._release())

    def test_mailmap_overrides_the_name(self):
        self._commit("gabriellafu <100+gabriellefu@users.noreply.github.com>")
        self._commit("Ann Lee <ann@example.com>", "Reviewers: gabriellafu (github:gabriellefu)")
        self.assertEqual((["Ann Lee", "Gabriella Fu"], []),
                         self._release("Gabriella Fu <100+gabriellefu@users.noreply.github.com>"))

    def test_mailmap_groups_people_that_share_nothing(self):
        self._commit("Avishek Das <avishek@example.com>",
                     "Co-authored-by: imavishek <789+imavishek@users.noreply.github.com>")
        self.assertEqual((["Avishek Das"], []),
                         self._release("Avishek Das <789+imavishek@users.noreply.github.com>"))

    def test_mailmap_separates_people_merged_by_a_wrong_email(self):
        self._commit("Sriram Subramanian <sriram@example.com>")
        self._commit("Swapnil Ghike <sriram@example.com>")
        self.assertEqual((["Sriram Subramanian", "Swapnil Ghike"], []),
                         self._release("Swapnil Ghike <swapnil@example.com> Swapnil Ghike <sriram@example.com>"))

    def test_reports_names_to_check(self):
        self._commit("Sriram Subramanian <sriram@example.com>")
        self._commit("Swapnil Ghike <sriram@example.com>")
        self._commit("Nick Telford <nick@example.com>")
        self._commit("NIck Telford <nick@example.com>")
        self._commit("Luke Chen <luke@example.com>")
        self._commit("showuon <luke@example.com>")
        self._commit("David Jacot <david@example.com>")
        self._commit("David Jaco <david@example.com>")
        self._commit("khyativ <kv@example.com>")
        self._commit("KhyatiVaghamshi <khyati@example.org>")
        lines = []
        self._release(report=lines.append)
        self.assertEqual(["Check that these names are of the same person:",
                          "  Swapnil Ghike <= Sriram Subramanian, Swapnil Ghike",
                          "Check whether these names are of the same person:",
                          "  khyativ, KhyatiVaghamshi"], lines[:-1])
        self.assertTrue(lines[-1].startswith("Fix any mistake in "))

    def test_reports_nothing_when_there_is_nothing_to_check(self):
        self._commit("Ann Lee <ann@example.com>")
        lines = []
        self._release(report=lines.append)
        self.assertEqual([], lines)


class AnnouncementEmailTest(unittest.TestCase):

    def test_lists_ais_apart_from_people(self):
        email = templates.release_announcement_email("4.4.0", ["ann lee", "Bo Kim"], ["Claude Opus 4.6", "Copilot"])
        self.assertIn("the following 2 contributors (and 2 AIs) to this release!", email)
        self.assertIn("\n\nann lee, Bo Kim\n\nAIs: Claude Opus 4.6, Copilot\n\n", email)
        email = templates.release_announcement_email("4.4.0", ["Bo Kim"], ["Copilot"])
        self.assertIn("the following 1 contributor (and 1 AI) to this release!", email)

    def test_leaves_out_ais_when_there_are_none(self):
        email = templates.release_announcement_email("4.4.0", ["Bo Kim"], [])
        self.assertIn("the following 1 contributor to this release!", email)
        self.assertIn("\n\nBo Kim\n\nWe welcome", email)
        self.assertNotIn("AIs", email)


if __name__ == "__main__":
    unittest.main()
