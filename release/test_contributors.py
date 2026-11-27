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
        self.assertEqual(("chickenchickenlove", None, "chickenchickenlove"),
                         parse("chickenchickenlove (@chickenchickenlove )"))
        self.assertEqual((None, None, "annlee"), parse("@annlee"))
        self.assertEqual((None, "ann@x", None), parse("ann@x"))
        self.assertEqual(("Ann Lee", None, None), parse("Ann Lee"))
        self.assertEqual(("Ann Lee", "ann@x", None), parse("Ann Lee ann@x"))
        self.assertEqual(("Ann Lee", "ann@x.org", None), parse("Ann Lee ann@x.org."))
        self.assertEqual(("Ann Lee", "ann@x", None), parse("Ann Lee ann@x>"))
        self.assertEqual(("Ann Lee", None, "annlee"), parse("Ann Lee @annlee"))
        self.assertEqual(("Ann Lee ann@x", None, None), parse("Ann Lee ann@x", bare_after_name=False))
        self.assertEqual((None, "ann@x", None), parse("ann@x", bare_after_name=False))
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

    def _head(self):
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.repo, text=True).strip()

    def _write_mailmap(self, *lines):
        with open(self.mailmap, "w") as f:
            f.write("".join(f"{line}\n" for line in lines))

    def _release(self, *mailmap_lines, report=None):
        self._write_mailmap(*mailmap_lines)
        self._git("tag", "2.0")
        return contributors.of_release("1.0", "2.0", mailmap=self.mailmap, cwd=self.repo, report=report)

    def test_credits_authors_co_authors_and_every_reviewer(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-authored-by: Bo Kim <bo@example.com>",
                     "Reviewers: Cy Park <cy@example.com>, Di Wu <di@example.com>, Ed Fox <ed@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu", "Ed Fox"], []), self._release())

    def test_credits_reviewers_before_a_separate_co_author_paragraph(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewers: Bo Kim <bo@example.com>, Cy Park <cy@example.com>",
                     "",
                     "Co-authored-by: Di Wu <di@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu"], []), self._release())

    def test_credit_headers_allow_case_and_spacing_variations(self):
        self._commit("Ann Lee <ann@example.com>",
                     "reviewers : Bo Kim <bo@example.com>",
                     "CO-AUTHORED-BY : Cy Park <cy@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park"], []), self._release())

    def test_reviewers_concatenated_after_a_co_author(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-Authored-By: Claude <noreply@anthropic.com> Reviewers: Andrew Schofield",
                     "<aschofield@confluent.io>")
        self.assertEqual((["Andrew Schofield", "Ann Lee"], ["Claude"]), self._release())

    def test_reviewers_concatenated_after_a_generated_by_credit(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Generated-by: OpenAI Codex Reviewers: Ken Huang <s7133700@gmail.com>")
        self.assertEqual((["Ann Lee", "Ken Huang"], ["OpenAI Codex"]), self._release())

    def test_cherry_pick_metadata_is_not_a_reviewer(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewers: Bo Kim <bo@example.com>",
                     "(cherry picked from commit 0123456789abcdef0123456789abcdef01234567)")
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release())

    def test_unfolds_indented_and_unindented_credit_lines(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewers: Bo Kim",
                     " <bo@example.com>, Cy Park",
                     "<cy@example.com>",
                     "",
                     "Co-authored-by: Di Wu",
                     " <di@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu"], []), self._release())

    def test_unfolds_an_email_without_inserting_a_space(self):
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim <bo@", "example.com>")
        credits = contributors.read_credits("HEAD", cwd=self.repo)
        reviewers = [(credit.name, credit.email) for credit in credits if credit.role == "reviewer"]
        self.assertEqual([("Bo Kim", "bo@example.com")], reviewers)

    def test_keeps_and_reports_an_ambiguous_bare_name_continuation(self):
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim <bo@example.com>,", "Cy Park")
        commit = self._head()
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park"], []), self._release(report=lines.append))
        report = "\n".join(lines)
        self.assertIn("Cy Park", report)
        self.assertIn(commit[:7], report)

    def test_does_not_credit_body_mentions_other_trailers_or_separate_prose(self):
        self._commit("Ann Lee <ann@example.com>",
                     "The description mentions Reviewers: Nobody <nobody@example.com>.",
                     "",
                     "Reviewer: Bo Kim <bo@example.com>",
                     "Signed-off-by: Cy Park",
                     " <cy@example.com>",
                     "",
                     "Di Wu <di@example.com>",
                     "",
                     "Co-authored-by: Ed Fox <ed@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Ed Fox"], []), self._release())

    def test_reports_prose_after_a_complete_reviewer_without_crediting_it(self):
        prose = "Please also check the release notes."
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim <bo@example.com>", prose)
        commit = self._head()
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release(report=lines.append))
        report = "\n".join(lines)
        self.assertIn(prose, report)
        self.assertIn(commit[:7], report)

    def test_reports_a_malformed_contact_and_keeps_previous_reviewers(self):
        malformed = "Broken [unclosed"
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim <bo@example.com>, " + malformed)
        commit = self._head()
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release(report=lines.append))
        report = "\n".join(lines)
        self.assertIn(malformed, report)
        self.assertIn(commit[:7], report)

    def test_ignores_punctuation_after_the_last_reviewer(self):
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim<bo@example.com>, Cy Park <cy@example.com>.")
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park"], []), self._release(report=lines.append))
        self.assertEqual([], lines)

    def test_does_not_credit_examples_in_completed_code_fences(self):
        self._commit("Ann Lee <ann@example.com>",
                     "```text",
                     "Reviewers: Nobody <nobody@example.com>",
                     "```",
                     "",
                     "~~~text",
                     "Reviewers: Someone Else <someone@example.com>",
                     "~~~",
                     "",
                     "Reviewers: Bo Kim <bo@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release())

    def test_unclosed_code_fence_does_not_hide_final_reviewers(self):
        self._commit("Ann Lee <ann@example.com>",
                     "```text",
                     "An example whose closing fence was omitted.",
                     "",
                     "Reviewers: Bo Kim <bo@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release())

    def test_markdown_boundaries_end_credit_continuations(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewer: Bo Kim <bo@example.com>",
                     "---",
                     "Ed Fox <ed@example.com>",
                     "",
                     "Reviewer: Cy Park <cy@example.com>",
                     "### Notes",
                     "Fay Lin <fay@example.com>",
                     "",
                     "Reviewer: Di Wu <di@example.com>",
                     "```text",
                     "Gus Hill <gus@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu"], []), self._release())

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
        lines = []
        self.assertEqual((["Gabriel Dutra", "Jiayáo Sun"], []), self._release(report=lines.append))
        self.assertEqual([], lines)

    def test_groups_github_logins_with_github_noreply_emails(self):
        self._commit("Uladzislau Blok <123+UladzislauBlok@users.noreply.github.com>",
                     "Reviewers: @UladzislauBlok, Mingi Cho (github:ChoMinGi)")
        self._commit("ChoMinGi <456+ChoMinGi@users.noreply.github.com>")
        self.assertEqual((["Mingi Cho", "Uladzislau Blok"], []), self._release())

    def test_groups_credits_with_an_email_or_login_without_brackets(self):
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim bo@example.com, Cy Park @cypark, Di Wu di@example.com.")
        self._commit("Bo Kim <bo@example.com>")
        self._commit("Cy Park <123+cypark@users.noreply.github.com>")
        self._commit("Di Wu <di@example.com>", "Co-authored-by: Ed Fox ed@example.com")
        self._commit("Ed Fox <ed@example.com>")
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim", "Cy Park", "Di Wu", "Ed Fox"], []), self._release(report=lines.append))
        self.assertEqual([], lines)

    def test_reports_text_with_a_bare_email_after_a_complete_reviewer(self):
        self._commit("Ann Lee <ann@example.com>", "Reviewers: Bo Kim <bo@example.com> thanks to cy@example.com",
                     "Send questions to di@example.com")
        lines = []
        self.assertEqual((["Ann Lee", "Bo Kim"], []), self._release(report=lines.append))
        self.assertEqual(2, len(lines))
        self.assertIn("thanks to cy@example.com", lines[0])
        self.assertIn("Send questions to di@example.com", lines[1])

    def test_does_not_group_github_logins_with_the_local_part_of_emails(self):
        self._commit("John Smith <john@example.com>", "Reviewers: @john")
        self._commit("John Jones <john@example.org>")
        self.assertEqual((["john", "John Jones", "John Smith"], []), self._release())

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

    def test_drops_bot_co_authors_without_reporting_them(self):
        self._commit("dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
                     "Reviewers: Ann Lee <ann@example.com>",
                     "",
                     "Signed-off-by: dependabot[bot] <support@github.com>",
                     "Co-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>")
        lines = []
        self.assertEqual((["Ann Lee"], []), self._release(report=lines.append))
        self.assertEqual([], lines)

    def test_credits_copilot_by_its_github_email(self):
        self._commit("Ann Lee <ann@example.com>", "Co-authored-by: GitHub Copilot <copilot@github.com>")
        self.assertEqual((["Ann Lee"], ["Copilot"]), self._release())

    def test_ai_co_authors_without_email_are_recognized_by_a_known_name(self):
        self._commit_before_release("Old Timer <old@example.com>",
                                    "Co-authored-by: Claude Opus 5.5 (1M context) <noreply@anthropic.com>")
        self._commit("Ann Lee <ann@example.com>",
                     "Co-Authored-By: Claude Opus 5.5 ",
                     "Reviewers: Bo Kim <bo@example.com>")
        self.assertEqual((["Ann Lee", "Bo Kim"], ["Claude Opus 5.5"]), self._release())

    def test_people_named_like_an_ai_are_not_dropped(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-authored-by: Claude Opus 5.5 <noreply@anthropic.com>",
                     "Co-authored-by: Claude Warren",
                     "Co-authored-by: Jean-Claude Dupont <jc@example.com>")
        self.assertEqual((["Ann Lee", "Claude Warren", "Jean-Claude Dupont"], ["Claude Opus 5.5"]), self._release())

    def test_codex_co_authors_without_email(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Co-authored-by: Bo Kim <bo@example.com>, OpenAI Codex (GPT-6)",
                     "Co-authored-by: Codex (GPT-5)",
                     "Co-authored-by: Codex")
        self.assertEqual((["Ann Lee", "Bo Kim", "Codex"], ["OpenAI Codex"]), self._release())

    def test_mailmap_overrides_the_name(self):
        self._commit("gabriellafu <100+gabriellefu@users.noreply.github.com>")
        self._commit("Ann Lee <ann@example.com>", "Reviewers: gabriellafu (github:gabriellefu)")
        self.assertEqual((["Ann Lee", "Gabriella Fu"], []),
                         self._release("Gabriella Fu <100+gabriellefu@users.noreply.github.com>"))

    def test_mailmap_resolves_email_less_github_logins(self):
        self._commit("Ann Lee <ann@example.com>",
                     "Reviewers: @AnnLee, annlee (github:annlee), A. Lee (@annlee)")
        self.assertEqual((["Ann Lee"], []),
                         self._release("Ann Lee <annlee@users.noreply.github.com>"))

    def test_mailmap_preserves_unmapped_email_less_github_logins(self):
        credits = [contributors.Credit("commit", "reviewer", *contributors.parse_person(person))
                   for person in ("@annlee", "annlee (github:annlee)", "A. Lee (@annlee)")]
        credits[-1].renamed = True
        expected = [(credit.name, credit.email, credit.login, credit.renamed) for credit in credits]
        self._write_mailmap("Bo Kim <bo@users.noreply.github.com>")
        mapped = contributors.apply_mailmap(credits, self.mailmap, cwd=self.repo)
        self.assertEqual(expected, [(credit.name, credit.email, credit.login, credit.renamed) for credit in mapped])

    def test_mailmap_does_not_treat_a_plain_name_as_a_github_login(self):
        self._commit("Bo Kim <bo@example.com>", "Reviewers: Ann")
        self.assertEqual((["Ann", "Bo Kim"], []),
                         self._release("Ann Lee <Ann@users.noreply.github.com>"))

    def test_mailmap_name_override_does_not_store_a_lookup_email(self):
        credit = contributors.Credit("commit", "reviewer", login="annlee")
        self._write_mailmap("Ann Lee <annlee@users.noreply.github.com>")
        mapped, = contributors.apply_mailmap([credit], self.mailmap, cwd=self.repo)
        self.assertEqual(("Ann Lee", None, "annlee", True),
                         (mapped.name, mapped.email, mapped.login, mapped.renamed))

    def test_mailmap_maps_email_less_login_to_a_canonical_email(self):
        credit = contributors.Credit("commit", "reviewer", login="annlee")
        self._write_mailmap("Ann Lee <ann@example.com> <annlee@users.noreply.github.com>")
        mapped, = contributors.apply_mailmap([credit], self.mailmap, cwd=self.repo)
        self.assertEqual(("Ann Lee", "ann@example.com", "annlee", True),
                         (mapped.name, mapped.email, mapped.login, mapped.renamed))

    def test_mailmap_prefers_an_existing_email_over_the_github_login(self):
        credit = contributors.Credit("commit", "reviewer", "Existing Name", "existing@example.com", "annlee")
        self._write_mailmap("Ann Lee <annlee@users.noreply.github.com>")
        mapped, = contributors.apply_mailmap([credit], self.mailmap, cwd=self.repo)
        self.assertEqual(("Existing Name", "existing@example.com", "annlee", False),
                         (mapped.name, mapped.email, mapped.login, mapped.renamed))

    def test_mailmap_groups_people_that_share_nothing(self):
        self._commit("Avishek Das <avishek@example.com>",
                     "Co-authored-by: imavishek <789+imavishek@users.noreply.github.com>")
        self.assertEqual((["Avishek Das"], []),
                         self._release("Avishek Das <789+imavishek@users.noreply.github.com>"))

    def test_mailmap_separates_people_merged_by_a_wrong_email(self):
        self._commit("Sriram Subramanian <sriram@example.com>")
        self._commit("Swapnil Ghike <sriram@example.com>")
        lines = []
        self.assertEqual((["Sriram Subramanian", "Swapnil Ghike"], []),
                         self._release("Swapnil Ghike <swapnil@example.com> Swapnil Ghike <sriram@example.com>",
                                       report=lines.append))
        self.assertEqual([], lines)

    def test_reports_commit_and_email_evidence_for_a_shared_word_name_collision(self):
        self._commit("David Jacot <shared@example.com>", "Reviewers: Jane Smith <review@example.com>")
        first_commit = self._head()
        self._commit("David Arthur <shared@example.com>", "Reviewers: Jane Jones <review@example.com>")
        second_commit = self._head()
        lines = []
        self._release(report=lines.append)
        evidence = [line.strip() for line in lines[lines.index("Commit evidence for grouped names:") + 1:-1]]
        for representative, expected in (
            ("David Arthur", [f"{first_commit[:12]} author: David Jacot <shared@example.com>",
                              f"{second_commit[:12]} author: David Arthur <shared@example.com>"]),
            ("Jane Jones", [f"{first_commit[:12]} reviewer: Jane Smith <review@example.com>",
                            f"{second_commit[:12]} reviewer: Jane Jones <review@example.com>"]),
        ):
            with self.subTest(representative=representative):
                heading = [line.rstrip(":") for line in evidence].index(representative)
                self.assertCountEqual(expected, evidence[heading + 1:heading + 3])

    def test_reports_names_to_check(self):
        self._commit("Sriram Subramanian <sriram@example.com>")
        self._commit("Swapnil Ghike <sriram@example.com>")
        self._commit("Luke Chen <luke@example.com>")
        self._commit("showuon <luke@example.com>")
        self._commit("David Jaco <david@example.com>")
        self._commit("David Jacot <david@example.com>")
        self._commit("Alieh Saeedi <alieh@example.com>")
        self._commit("aliehsaeedii <alieh@example.com>")
        self._commit("John Smith <john@example.com>")
        self._commit("John Jones <john@example.com>")
        self._commit("Nick Telford <nick@example.com>")
        self._commit("NIck Telford <nick@example.com>")
        self._commit("khyativ <kv@example.com>")
        self._commit("KhyatiVaghamshi <khyati@example.org>")
        lines = []
        self._release(report=lines.append)
        unrelated = lines.index("Check that these names are of the same person:")
        variants = lines.index("These names look like typos or variants of one name, and were grouped too:")
        similar = lines.index("Check whether these names are of the same person:")
        evidence = lines.index("Commit evidence for grouped names:")
        self.assertEqual(0, unrelated)
        # Names with nothing in common come first, as that is what a wrong email looks like.
        self.assertEqual(["  Luke Chen <= Luke Chen, showuon",
                          "  Swapnil Ghike <= Sriram Subramanian, Swapnil Ghike"],
                         lines[unrelated + 1:variants])
        self.assertEqual(["  Alieh Saeedi <= Alieh Saeedi, aliehsaeedii",
                          "  David Jacot <= David Jaco, David Jacot",
                          "  John Jones <= John Jones, John Smith"],
                         lines[variants + 1:similar])
        # Names that were not grouped, but look alike.
        self.assertEqual(["  khyativ, KhyatiVaghamshi"], lines[similar + 1:evidence])
        # The evidence follows the order of the groups above.
        headings = [line.strip().rstrip(":") for line in lines[evidence + 1:-1] if not line.startswith("    ")]
        self.assertEqual(["Luke Chen", "Swapnil Ghike", "Alieh Saeedi", "David Jacot", "John Jones"], headings)
        # Names that only differ in case are not reported.
        self.assertFalse(any("Telford" in line for line in lines))
        self.assertEqual(f"Fix any mistake in {self.mailmap}", lines[-1])

    def test_reports_nothing_when_there_is_nothing_to_check(self):
        self._commit("Ann Lee <ann@example.com>")
        lines = []
        self._release(report=lines.append)
        self.assertEqual([], lines)


class AnnouncementEmailTest(unittest.TestCase):

    def test_lists_the_contributors(self):
        email = templates.release_announcement_email("4.4.0", ["ann lee", "Bo Kim"])
        self.assertIn("the following 2 contributors to this release!", email)
        self.assertIn("\n\nann lee, Bo Kim\n\nWe welcome", email)

    def test_formats_a_single_contributor(self):
        email = templates.release_announcement_email("4.4.0", ["Bo Kim"])
        self.assertIn("the following 1 contributor to this release!", email)
        self.assertIn("\n\nBo Kim\n\nWe welcome", email)


if __name__ == "__main__":
    unittest.main()
