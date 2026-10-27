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
Builds the list of contributors thanked in the release announcement.

A release credits everyone who authored, co-authored or reviewed one of its commits, as
recorded by the commit author and the Co-authored-by and Reviewers trailers. These are free
text, so the same person often shows up under several names and emails, for example
"OmniaGM" and "Omnia Ibrahim", or with a typo typed by a reviewer. The list is built in steps:

  1. read_credits(): read every credit from the history up to the release. Older commits
     are read too, as they help to recognize the people of the release.
  2. apply_mailmap(): apply the name overrides of contributors.mailmap.
  3. Set aside the AI tools, which are counted separately, and drop the bots.
  4. group_people(): group the credits of the same person.
  5. best_name(): pick the name to show for each person.

As the grouping can be wrong without it showing in the list, names_to_check() also tells the
release manager which names may have been merged by mistake and which look alike but were not
merged.

To show someone with another name, or to fix a wrong grouping, add a line to contributors.mailmap.
To recognize a new AI tool, add its email to AI_TOOLS.
"""

import itertools
import os
import re
import subprocess
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Optional

from runtime import repo_dir

# Name overrides, in git mailmap format. See the file for when to add an entry.
MAILMAP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contributors.mailmap")

# Trailers that credit someone, with the role they give to that person.
CREDIT_TRAILERS = {
    "co-authored-by": "author",
    "generated-by": "ai",
    "reviewers": "reviewer",
    "reviewer": "reviewer",
}

# AI tools, recognized by the email in their trailer, with the name to show them with.
# None shows the name of the trailer without a suffix in parentheses, so that each model is
# shown once, e.g. "Claude Opus 4.6 (1M context)" is shown as "Claude Opus 4.6".
# Match the exact addresses used by the tools, not a whole domain, as people of the same
# company may contribute too.
AI_TOOLS = [
    (r"^(noreply|claude)@anthropic\.com$", None),
    (r"^(\d+\+)?copilot@users\.noreply\.github\.com$", "Copilot"),
    (r"^cursoragent@cursor\.com$", "Cursor"),
]

# Names that do not identify a person, so they must not be used to match people.
PLACEHOLDER_NAMES = {"anonymous", "unknown"}

GITHUB_NOREPLY_EMAIL = re.compile(r"^(?:\d+\+)?([^@]+)@users\.noreply\.github\.com$")

# One person as written in a trailer: "Name <email>", "Name <email" (not closed),
# "Name [email](mailto:email)", "Name (github:login)", "Name (@login)", or only "Name".
PERSON = re.compile(r"""
    ^(?:"(?P<quoted>[^"]*)"|(?P<name>[^<\[]*?))\s*
    (?:<(?P<email>[^<>]*)>?
      |\[(?P<md_email>[^\]]*)\]\(mailto:[^)]*\)
      |\((?:github:|@)(?P<login>[^)\s]+)\))?
    $""", re.VERBOSE)


@dataclass
class Credit:
    commit: str
    role: str                     # "author", "reviewer" or "ai"
    name: Optional[str] = None
    email: Optional[str] = None
    login: Optional[str] = None   # GitHub login
    renamed: bool = False         # whether the name comes from the mailmap

    def github_login(self):
        if self.login:
            return self.login
        noreply = GITHUB_NOREPLY_EMAIL.match(self.email.lower()) if self.email else None
        return noreply.group(1) if noreply else None

    def shown_name(self):
        return self.name or self.github_login() or self.email


def of_release(from_rev, to_rev, mailmap=MAILMAP, cwd=repo_dir, report=None):
    """
    Return (people, ais): the sorted names of the people credited by the commits in
    from_rev..to_rev, and of the AI tools credited by those commits.

    If report is given, it is called with each line of what the release manager should check,
    see names_to_check().
    """
    release_commits = set(_git(["rev-list", "--no-merges", f"{from_rev}..{to_rev}"], cwd).split())
    credits = apply_mailmap(read_credits(to_rev, cwd), mailmap, cwd)

    ais = set()
    humans = []
    for credit in credits:
        ai_name = _ai_tool_name(credit)
        if ai_name or credit.role == "ai":
            if ai_name and credit.commit in release_commits:
                ais.add(ai_name)
        elif not _is_bot(credit):
            credit.name = _clean_name(credit.name)
            humans.append(credit)

    groups = [group for group in group_people(humans) if any(credit.commit in release_commits for credit in group)]
    if report:
        for line in names_to_check(groups, release_commits):
            report(line)
    people = {best_name(group, release_commits) for group in groups}
    return sorted(people, key=str.casefold), sorted(ais, key=str.casefold)


def read_credits(rev, cwd=repo_dir):
    """
    Return the credits of the non-merge commits reachable from rev, from the newest commit.
    """
    log = _git(["log", "--no-merges", "--format=%H%x1f%an%x1f%ae%x1f%(trailers:unfold)%x1e", rev], cwd)
    credits = []
    for record in log.split("\x1e"):
        if not record.strip():
            continue
        commit, name, email, trailers = record.strip("\n").split("\x1f")
        credits.append(Credit(commit, "author", name, email))
        for trailer in trailers.splitlines():
            key, _, value = trailer.partition(":")
            role = CREDIT_TRAILERS.get(key.strip().lower())
            if role == "reviewer":
                # Reviewers lists several people, and the value sometimes repeats "Reviewers:".
                value = re.sub(r"^\s*reviewers?\s*:", "", value, flags=re.IGNORECASE)
                credits += [Credit(commit, role, *parse_person(person)) for person in split_people(value)]
            elif role:
                credits.append(Credit(commit, role, *parse_person(value)))
    return credits


def split_people(value):
    """
    Split a list of people such as 'A <a@x>, "B, C" <b@x>, D' into one string per person.
    Commas inside quotes do not separate people, and a closing '>' always ends a person.
    """
    people, current, quoted = [], "", False
    for c in value:
        if c == '"':
            quoted = not quoted
        elif c == "," and not quoted:
            people.append(current)
            current = ""
            continue
        current += c
        if c == ">" and not quoted:
            people.append(current)
            current = ""
    people.append(current)
    return [person.strip() for person in people if person.strip()]


def parse_person(text):
    """
    Return (name, email, login) for one person, any of which may be None.
    """
    match = PERSON.match(text.strip())
    if match is None:
        return text.strip() or None, None, None
    name = match.group("quoted") if match.group("quoted") is not None else match.group("name")
    name = name.strip() or None
    email = (match.group("email") or match.group("md_email") or "").strip() or None
    login = match.group("login")
    if name and not email and not login:
        if name.startswith("@"):
            name, login = None, name[1:]
        elif re.fullmatch(r"[^@\s]+@[^@\s]+", name):
            name, email = None, name
    return name, email, login


def apply_mailmap(credits, mailmap, cwd=repo_dir):
    """
    Replace names and emails as told by the mailmap. This uses git check-mailmap, as git log
    does not apply a mailmap to trailers.
    """
    idents = list({(credit.name, credit.email) for credit in credits if credit.email})
    contacts = "".join(f"{name} <{email}>\n" if name else f"<{email}>\n" for name, email in idents)
    output = _git(["-c", f"mailmap.file={mailmap}", "check-mailmap", "--stdin"], cwd, input=contacts)
    lines = output.splitlines()
    if len(lines) != len(idents):
        raise RuntimeError(f"git check-mailmap returned {len(lines)} contacts instead of {len(idents)}")
    mapped = {}
    for ident, line in zip(idents, lines):
        name, _, email = line.rpartition("<")
        mapped[ident] = (name.strip() or None, email.rstrip(">"))
    for credit in credits:
        if credit.email:
            name, credit.email = mapped[(credit.name, credit.email)]
            credit.renamed = name != credit.name
            credit.name = name
    return credits


def group_people(credits):
    """
    Group the credits of the same person, which share an email, a normalized name or a GitHub
    login with each other, directly or through other credits of that person.
    """
    logins = {_normalize(credit.github_login()) for credit in credits if credit.github_login()}
    parent = {}

    def root(key):
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    first_keys = []
    for credit in credits:
        keys = _identity_keys(credit, logins)
        for key in keys[1:]:
            parent[root(key)] = root(keys[0])
        first_keys.append(keys[0] if keys else None)

    # Each group keeps the order of the credits, from the newest commit.
    people = defaultdict(list)
    for credit, key in zip(credits, first_keys):
        if key:
            people[root(key)].append(credit)
    return list(people.values())


def best_name(credits, release_commits):
    """
    Return the name to show for the credits of one person, preferring in this order:
      1. a name from the mailmap,
      2. a real name ("Omnia Ibrahim") over a handle ("OmniaGM", "nileshkumar3"),
      3. a name the person used themselves over one typed by a reviewer, which has more typos,
      4. the name used the most in the release, then the most overall, then the most recently.
    """
    from_mailmap, self_reported, in_release, total, first_seen = set(), set(), Counter(), Counter(), {}
    for index, credit in enumerate(credits):
        name = credit.shown_name()
        if credit.renamed:
            from_mailmap.add(name)
        if credit.role != "reviewer":
            self_reported.add(name)
        in_release[name] += credit.commit in release_commits
        total[name] += 1
        first_seen.setdefault(name, index)

    def preference(name):
        looks_real = " " in name and not re.search(r"\d", name)
        # Credits are ordered from the newest commit, so the first seen is the most recent.
        return (name in from_mailmap, looks_real, name in self_reported,
                in_release[name], total[name], -first_seen[name])

    return max(total, key=preference)


def names_to_check(groups, release_commits):
    """
    Return the lines that tell the release manager where the grouping may be wrong, as it can
    go wrong both ways without anything looking off in the list:
      - a wrong email in a single commit merges two people, so that one of them is not listed,
      - people that share no email, name or login are not merged, so they are listed twice.
    """
    merged = []
    for group in groups:
        names = sorted({credit.shown_name() for credit in group}, key=str.casefold)
        if _may_be_different_people(names):
            merged.append(f"  {best_name(group, release_commits)} <= {', '.join(names)}")

    shown = sorted({best_name(group, release_commits) for group in groups}, key=str.casefold)
    similar = [f"  {a}, {b}" for a, b in itertools.combinations(shown, 2) if _may_be_same_person(a, b)]

    lines = []
    if merged:
        lines += ["Check that these names are of the same person:"] + sorted(merged, key=str.casefold)
    if similar:
        lines += ["Check whether these names are of the same person:"] + similar
    if lines:
        lines.append(f"Fix any mistake in {os.path.relpath(MAILMAP, repo_dir)}")
    return lines


def _may_be_different_people(names):
    # Almost every group has several names, such as a login next to a full name ("showuon" and
    # "Luke Chen") or a typo in one word ("David Jaco" and "David Jacot"), and listing them all
    # would hide the wrong ones. Two full names that share no word, such as "Sriram Subramanian"
    # and "Swapnil Ghike", are what a wrong email looks like.
    words = [{_normalize(word) for word in name.split()} - {""} for name in names]
    full_names = [name_words for name_words in words if len(name_words) > 1]
    return any(not a & b for a, b in itertools.combinations(full_names, 2))


def _may_be_same_person(a, b):
    # E.g. "khyativ" and "KhyatiVaghamshi", or "manan.gupta" and "manangupta111".
    a, b = sorted((_normalize(a), _normalize(b)), key=len)
    return len(a) >= 5 and b.startswith(a)


def _identity_keys(credit, logins):
    """
    Return the keys that identify the person of a credit. Names and logins share keys, as many
    people use their login as their name.
    """
    keys = []
    if credit.email:
        keys.append("email:" + credit.email.lower())
        # Many people use their login as the local part of their email, which is the only way to
        # match a reviewer written as "@login". It is only used when some credit has that login,
        # as different people may use the same local part with different domains.
        local_part = _normalize(credit.email.split("@")[0])
        if local_part in logins:
            keys.append("name:" + local_part)
    for name in (credit.name, credit.github_login()):
        if name and _normalize(name) and _normalize(name) not in PLACEHOLDER_NAMES:
            keys.append("name:" + _normalize(name))
    return keys


def _normalize(name):
    """
    Reduce a name to lowercase letters and digits, so that "Jiayáo Sun" and "jiayaosun" match.
    """
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    return re.sub(r"[\W_]", "", name.casefold())


def _clean_name(name):
    """
    Remove commas, which would look like two people in the list, extra spaces, and what is left
    of the trailer before the name, such as "@", ". ", "and " or "Reviewers:".
    """
    if not name:
        return None
    name = re.sub(r"\s+", " ", name.replace(",", " ")).strip()
    return re.sub(r"^(?:(?i:reviewers?\s*:)|and\s|&|[^\w(\"])+\s*", "", name) or None


def _ai_tool_name(credit):
    """
    Return the name to show for the AI tool of a credit, or None if it is not for a known AI tool.
    """
    for email_pattern, name in AI_TOOLS:
        if credit.email and re.search(email_pattern, credit.email.lower()):
            return name or _without_parentheses(credit.name)
    if credit.role == "ai":
        return _without_parentheses(credit.name)
    return None


def _without_parentheses(name):
    return re.sub(r"\s*\([^)]*\)", "", name or "").strip() or None


def _is_bot(credit):
    return "[bot]" in f"{credit.name} {credit.email}"


def _git(args, cwd, input=None):
    # Unlike runtime.execute(), this keeps stderr out of the output, so that a warning from git
    # cannot be read as a commit or a contact. It is shown to the user instead.
    return subprocess.run(["git"] + args, cwd=cwd, input=input, stdout=subprocess.PIPE,
                          encoding="utf-8", check=True).stdout
