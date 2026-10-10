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
Release schedule helper for release managers and committers.

Projects KIP freeze / feature freeze / code freeze / release dates of the
upcoming releases from what actually happened in past releases (release branch
cuts and RC/release tags in git), so a release plan starts from realistic dates
instead of the nominal "code freeze + 4 weeks".

Only uses the python standard library, so it can be run without the release
virtualenv:

    python release/schedule.py              # next 3 releases, same as "plan"
    python release/schedule.py status       # the next release
    python release/schedule.py status 4.6.0
    python release/schedule.py history

See README.md for details.
"""

import argparse
import datetime
import functools
import gzip
import html
import http.client
import itertools
import json
import math
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request

repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

JIRA_SEARCH_URL = "https://issues.apache.org/jira/rest/api/2/search"
WIKI_CONTENT_URL = "https://cwiki.apache.org/confluence/rest/api/content"

# Used when no published release plan is available to take the phase lengths from.
DEFAULT_OFFSET_WEEKS = {"feature_freeze_weeks": 3, "code_freeze_weeks": 2, "target_weeks": 4}

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def parse_date(value):
    return datetime.date.fromisoformat(value)


def parse_assignments(values):
    """Parses repeated "VERSION=YYYY-MM-DD" arguments into a dict."""
    result = {}
    for value in values or []:
        version, _, date = value.partition("=")
        if not date:
            raise argparse.ArgumentTypeError(f"Expected VERSION=YYYY-MM-DD, got '{value}'")
        result[version.strip()] = parse_date(date.strip())
    return result


def wednesday_of_iso_week(year, week):
    return datetime.date.fromisocalendar(year, week, 3)


def next_kip_freeze_slot(after, iso_weeks):
    """Returns the first rule slot (Wednesday of one of the ISO weeks) strictly after the given date."""
    for year in range(after.year, after.year + 3):
        for week in sorted(iso_weeks):
            slot = wednesday_of_iso_week(year, week)
            if slot > after:
                return slot
    raise ValueError(f"No KIP freeze slot found after {after}")


def percentile(values, pct):
    """Nearest-rank percentile; good enough for the handful of data points we have."""
    if not values:
        raise ValueError("No values")
    ordered = sorted(values)
    rank = max(1, -(-len(ordered) * pct // 100))  # ceil without importing math
    return ordered[int(rank) - 1]


def new_moon(k):
    """
    Time (UTC, ignoring the ~1 minute TT/UT difference) of the k-th new moon after January 2000, using the
    main periodic terms from Meeus, "Astronomical Algorithms", chapter 49. Accurate to a few minutes.
    """
    t = k / 1236.85
    e = 1 - 0.002516 * t
    m = math.radians(2.5534 + 29.10535670 * k)
    mp = math.radians(201.5643 + 385.81693528 * k)
    f = math.radians(160.7108 + 390.67050284 * k)
    om = math.radians(124.7746 - 1.56375588 * k)
    jde = (2451550.09766 + 29.530588861 * k + 0.00015437 * t * t
           - 0.40720 * math.sin(mp) + 0.17241 * e * math.sin(m) + 0.01608 * math.sin(2 * mp)
           + 0.01039 * math.sin(2 * f) + 0.00739 * e * math.sin(mp - m) - 0.00514 * e * math.sin(mp + m)
           + 0.00208 * e * e * math.sin(2 * m) - 0.00111 * math.sin(mp - 2 * f) - 0.00057 * math.sin(mp + 2 * f)
           + 0.00056 * e * math.sin(2 * mp + m) - 0.00042 * math.sin(3 * mp) + 0.00042 * e * math.sin(m + 2 * f)
           + 0.00038 * e * math.sin(m - 2 * f) - 0.00024 * e * math.sin(2 * mp - m) - 0.00017 * math.sin(om))
    return datetime.datetime(2000, 1, 1, 12) + datetime.timedelta(days=jde - 2451545.0)


def lunar_new_year(year):
    """Lunar New Year is the first new moon (in China, UTC+8) on or after January 21."""
    k = math.floor((year + 20 / 365.25 - 2000) * 12.3685)
    while True:
        day = (new_moon(k) + datetime.timedelta(hours=8)).date()
        if day >= datetime.date(year, 1, 21):
            return day
        k += 1


def holiday_windows(year):
    lny = lunar_new_year(year)
    return [("Christmas/New Year", datetime.date(year, 12, 20), datetime.date(year + 1, 1, 3)),
            ("Lunar New Year", lny - datetime.timedelta(days=3), lny + datetime.timedelta(days=7))]


def holiday_conflicts(name, start, end):
    """Returns holidays overlapping [start, end] as human readable notes."""
    notes = []
    for year in range(start.year - 1, end.year + 1):
        for holiday, h_start, h_end in holiday_windows(year):
            if h_start <= end and start <= h_end:
                notes.append(f"{name} overlaps {holiday} ({h_start} .. {h_end})")
    return notes


def bump_minor(version):
    major, minor, _ = (int(p) for p in version.split("."))
    return f"{major}.{minor + 1}.0"


# ---------------------------------------------------------------------------
# History (from git)
# ---------------------------------------------------------------------------

def git(*args):
    return subprocess.check_output(["git", *args], cwd=repo_dir, text=True).strip()


def detect_remote(preferred):
    remotes = git("remote").split()
    if preferred:
        if preferred not in remotes:
            sys.exit(f"Remote '{preferred}' not found, available: {', '.join(remotes)}")
        return preferred
    for remote in remotes:
        url = git("remote", "get-url", remote)
        if re.search(r"apache/kafka(\.git)?$", url):
            return remote
    sys.exit("Could not find a remote pointing at apache/kafka, use --remote")


def ref_date(ref):
    out = git("for-each-ref", "--format=%(creatordate:short)", ref)
    if out:
        return parse_date(out.splitlines()[0])
    return parse_date(git("log", "-1", "--format=%cs", ref))


def version_key(version):
    return tuple(int(p) for p in version.split("."))


def release_branches(remote, since="0.0"):
    branches = []
    for ref in git("for-each-ref", "--format=%(refname:short)", f"refs/remotes/{remote}/").splitlines():
        name = ref[len(remote) + 1:]
        if re.fullmatch(r"\d+\.\d+", name) and version_key(name) >= version_key(since):
            branches.append(name)
    if not branches:
        sys.exit(f"No release branches found on remote '{remote}', run: git fetch {remote} --tags")
    return sorted(branches, key=version_key)


def next_release(remote):
    """The first release without a release branch yet, i.e. the one trunk is currently heading for."""
    return bump_minor(f"{release_branches(remote)[-1]}.0")


def collect_history(remote, since, release_dates, window=None):
    """
    Builds one record per x.y release branch on the remote.

    The release branch is cut at feature freeze, so the branch point is used as the feature freeze
    date. The x.y.0 tag date (or an explicit --release-date) is used as the release date.
    """
    branches = release_branches(remote, since or "0.0")
    if window:
        branches = branches[-window:]

    tags = git("tag", "-l").splitlines()
    trunk = f"{remote}/trunk"
    history = []
    for branch in branches:
        version = f"{branch}.0"
        branch_ref = f"{remote}/{branch}"
        merge_base = git("merge-base", trunk, branch_ref)
        cut = parse_date(git("log", "-1", "--format=%cs", merge_base))

        rcs = sorted((int(m.group(1)), t) for t in tags
                     if (m := re.fullmatch(re.escape(version) + r"-rc(\d+)", t)))
        # Older releases only kept the final RC tag, in which case the first RC is unknown.
        has_rc0 = bool(rcs) and rcs[0][0] == 0
        first_rc = ref_date(f"refs/tags/{rcs[0][1]}") if has_rc0 else None

        released = release_dates.get(version)
        end_ref = branch_ref
        if version in tags:
            end_ref = f"refs/tags/{version}"
            released = released or ref_date(end_ref)
        elif rcs:
            end_ref = f"refs/tags/{rcs[-1][1]}"

        history.append({
            "version": version,
            "feature_freeze": cut,
            "first_rc": first_rc,
            "released": released,
            "released_given": version in release_dates,
            "rc_count": rcs[-1][0] + 1 if rcs else 0,
            "commits_after_cut": int(git("rev-list", "--count", f"{merge_base}..{end_ref}")),
            "commits_after_first_rc": int(git("rev-list", "--count", f"refs/tags/{rcs[0][1]}..{end_ref}"))
            if has_rc0 else None,
        })
    return history


def recent(history, window):
    return history[-window:] if window else history


def history_stats(history, window):
    """Median-able durations (in days) over the recent releases."""
    entries = recent(history, window)
    released = [h for h in entries if h["released"]]
    with_rc0 = [h for h in released if h["first_rc"]]
    return {
        "versions": [h["version"] for h in entries],
        "ff_to_release": [(h["released"] - h["feature_freeze"]).days for h in released],
        "ff_to_rc0": [(h["first_rc"] - h["feature_freeze"]).days for h in with_rc0],
        "rc0_to_release": [(h["released"] - h["first_rc"]).days for h in with_rc0],
        "ff_interval": [(b["feature_freeze"] - a["feature_freeze"]).days for a, b in zip(entries, entries[1:])],
        "rc_count": [h["rc_count"] for h in released],
        "planned_ff_to_cut": [(h["feature_freeze"] - h["planned"]["feature_freeze"]).days
                              for h in entries if h.get("planned")],
        "target_to_release": [(h["released"] - h["planned"]["target"]).days
                              for h in released if h.get("planned")],
    }


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------

def wednesday_on_or_before(date):
    return date - datetime.timedelta(days=(date.weekday() - 2) % 7)


def build_plan(after_version, after_release, after_feature_freeze, count, ff_weeks, cf_weeks, target_weeks,
               ff_interval_days, ff_to_rc0_days, ff_to_release_days, kip_freeze_overrides,
               iso_weeks=None, versions=None, avoid_holidays=True, short_cycle_weeks=12, published=None):
    """
    Projects the next `count` releases.

    KIP freeze, in order of precedence:
      - an explicit override (a date already agreed on dev@),
      - the published release plan on the wiki, which also provides feature freeze, code freeze and target,
      - the fixed calendar rule if `iso_weeks` is given (Wednesday of one of those ISO weeks),
      - otherwise derived from history: previous feature freeze + typical feature freeze interval,
        minus `ff_weeks`, and moved later week by week while a milestone falls into a holiday window.

    target release    = code freeze + target_weeks, or the published target (what the release plan promises)
    realistic release = feature freeze + typical feature-freeze-to-release duration (what history says)
    """
    plan = []
    prev = {"version": after_version, "realistic": after_release, "kip_freeze": None,
            "feature_freeze": after_feature_freeze}
    version = after_version
    for i in range(count):
        version = versions[i] if versions and i < len(versions) else bump_minor(version)

        def milestones(kip_freeze):
            ff = kip_freeze + datetime.timedelta(weeks=ff_weeks)
            cf = ff + datetime.timedelta(weeks=cf_weeks)
            nominal = cf + datetime.timedelta(weeks=target_weeks)
            return {"kip_freeze": kip_freeze, "feature_freeze": ff, "code_freeze": cf, "nominal": nominal,
                    "first_rc": ff + datetime.timedelta(days=ff_to_rc0_days),
                    "realistic": max(nominal, ff + datetime.timedelta(days=ff_to_release_days))}

        def point_conflicts(m):
            return [n for key, label in (("kip_freeze", "KIP freeze"), ("feature_freeze", "feature freeze"),
                                         ("code_freeze", "code freeze"), ("first_rc", "first RC"),
                                         ("realistic", "realistic release"))
                    for n in holiday_conflicts(label, m[key], m[key])]

        if version in kip_freeze_overrides:
            entry = milestones(kip_freeze_overrides[version])
            source = "given"
        elif published and version in published:
            plan_dates = published[version]
            entry = {"kip_freeze": plan_dates["kip_freeze"], "feature_freeze": plan_dates["feature_freeze"],
                     "code_freeze": plan_dates["code_freeze"], "nominal": plan_dates["target"],
                     "first_rc": plan_dates["feature_freeze"] + datetime.timedelta(days=ff_to_rc0_days),
                     "realistic": max(plan_dates["target"], plan_dates["feature_freeze"] +
                                      datetime.timedelta(days=ff_to_release_days))}
            source = "wiki release plan"
        elif iso_weeks:
            entry = milestones(next_kip_freeze_slot(prev["kip_freeze"] or prev["feature_freeze"], iso_weeks))
            source = f"rule: ISO week {entry['kip_freeze'].isocalendar()[1]}"
        else:
            target_ff = prev["feature_freeze"] + datetime.timedelta(days=ff_interval_days)
            entry = milestones(wednesday_on_or_before(target_ff - datetime.timedelta(weeks=ff_weeks)))
            source = "history"
            if avoid_holidays and point_conflicts(entry):
                # Move by as few weeks as possible, preferring later on a tie, but never put the KIP freeze
                # before the previous release has (realistically) shipped.
                earliest = prev["realistic"] or prev["feature_freeze"]
                for shift in (s for n in range(1, 13) for s in (n, -n)):
                    candidate = milestones(entry["kip_freeze"] + datetime.timedelta(weeks=shift))
                    if candidate["kip_freeze"] > earliest and not point_conflicts(candidate):
                        entry = candidate
                        source += f", {shift:+d}w for holidays"
                        break

        warnings = []
        if prev["realistic"]:
            if entry["feature_freeze"] <= prev["realistic"]:
                warnings.append(f"feature freeze {entry['feature_freeze']} is before {prev['version']} "
                                f"realistically ships ({prev['realistic']}): two release branches stabilizing "
                                f"at once")
            elif entry["kip_freeze"] <= prev["realistic"]:
                warnings.append(f"KIP freeze {entry['kip_freeze']} is before {prev['version']} realistically "
                                f"ships ({prev['realistic']})")
            cycle_weeks = (entry["realistic"] - prev["realistic"]).days / 7
            if cycle_weeks < short_cycle_weeks:
                warnings.append(f"only {cycle_weeks:.0f} weeks after {prev['version']}: very small release")
        warnings += point_conflicts(entry)
        warnings += holiday_conflicts("RC/vote window", entry["code_freeze"], entry["realistic"])

        entry.update({"version": version, "kip_freeze_source": source, "warnings": warnings})
        plan.append(entry)
        prev = entry
    return plan


# ---------------------------------------------------------------------------
# JIRA
# ---------------------------------------------------------------------------

def get_json(url, attempts=3):
    """
    GETs a JSON document. Asks for gzip, as large uncompressed responses (e.g. wiki pages) are sometimes cut
    off mid-transfer, and retries a few times if that still happens.
    """
    request = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", "Accept": "application/json"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
                return json.loads(body)
        except http.client.IncompleteRead:
            if attempt == attempts - 1:
                raise


def jira_search(jql, fields="key,summary,priority,assignee,status,updated", max_results=50):
    query = urllib.parse.urlencode({"jql": jql, "fields": fields, "maxResults": max_results})
    return get_json(f"{JIRA_SEARCH_URL}?{query}")


# ---------------------------------------------------------------------------
# Wiki release plans
# ---------------------------------------------------------------------------

MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_DAY = r"(\d{1,2})(?:st|nd|rd|th)?"
_MONTH = r"([A-Za-z]{3,9})\.?"
_YEAR = r",?\s*(\d{4})?"
# "8th July 2026", "20th November 2024" / "February 25 2026", "January 29th 2025", "May 14", "March 17, 2027"
DAY_FIRST = re.compile(rf"{_DAY}\s+{_MONTH}{_YEAR}")
MONTH_FIRST = re.compile(rf"{_MONTH}\s+{_DAY}{_YEAR}")
# Labels of the "Release Dates" section of the release plan template, in chronological order
PLAN_LABELS = (("kip_freeze", "KIP Freeze:"), ("feature_freeze", "Feature Freeze:"),
               ("code_freeze", "Code Freeze:"), ("target", "no earlier than"))


def parse_loose_date(text):
    """Parses the date at the start of `text` into (month, day, year or None)."""
    text = text.lstrip(" (")
    for pattern, day_group, month_group in ((DAY_FIRST, 1, 2), (MONTH_FIRST, 2, 1)):
        m = pattern.match(text)
        month = m and MONTHS.get(m.group(month_group)[:3].lower())
        if month:
            return month, int(m.group(day_group)), int(m.group(3)) if m.group(3) else None
    return None


def parse_release_plan(page_html, reference):
    """
    Extracts KIP freeze, feature freeze, code freeze and target release date from the "Release Dates"
    section of a release plan wiki page. Dates written without a year get the year closest to `reference`.
    Returns None if the section does not follow the template.
    """
    text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", page_html)))
    plan, prev, pos = {}, None, 0
    for key, label in PLAN_LABELS:
        start = text.find(label, pos)
        if start < 0:
            return None
        pos = start + len(label)
        parsed = parse_loose_date(text[pos:pos + 40])
        if not parsed:
            return None
        month, day, year = parsed
        try:
            candidates = [datetime.date(y, month, day) for y in ((year,) if year else
                          (reference.year - 1, reference.year, reference.year + 1))]
        except ValueError:
            return None
        date = min(candidates, key=lambda d: abs((d - reference).days))
        # The milestones are chronological, which also fixes an inferred year around new year
        while prev and date < prev and not year:
            date = date.replace(year=date.year + 1)
        plan[key] = prev = date
    return plan


@functools.lru_cache(maxsize=None)
def fetch_release_plan(version, reference=None):
    """Reads the "Release Plan X.Y.Z" page from the Kafka wiki, None if it does not exist or can't be parsed."""
    query = urllib.parse.urlencode({"spaceKey": "KAFKA", "title": f"Release Plan {version}",
                                    "expand": "body.storage,history"})
    content = get_json(f"{WIKI_CONTENT_URL}?{query}")
    if not content.get("results"):
        return None
    page = content["results"][0]
    if not reference:
        # Plans are written a few months ahead of the dates they contain
        reference = parse_date(page["history"]["createdDate"][:10]) + datetime.timedelta(days=90)
    plan = parse_release_plan(page["body"]["storage"]["value"], reference)
    if plan:
        plan["url"] = content["_links"]["base"] + page["_links"]["webui"]
    return plan


def fetch_release_plans(versions, references, args):
    if args.no_wiki:
        return {}
    plans = {}
    for version in versions:
        try:
            plan = fetch_release_plan(version, references.get(version))
        except (OSError, ValueError, KeyError, http.client.HTTPException) as e:
            print(f"Note: could not read the {version} release plan from the wiki ({e!r}), "
                  f"rerun with --no-wiki to skip", file=sys.stderr)
            continue
        if plan:
            plans[version] = plan
    return plans


def resolve_offsets(args, history):
    """
    Phase lengths (in weeks) not given on the command line are taken from the most recent published
    release plan, so the projection follows what the community currently plans with.
    """
    latest = next((h for h in reversed(history) if h.get("planned")), None)
    for key, (start, end) in (("feature_freeze_weeks", ("kip_freeze", "feature_freeze")),
                              ("code_freeze_weeks", ("feature_freeze", "code_freeze")),
                              ("target_weeks", ("code_freeze", "target"))):
        if getattr(args, key) is None:
            weeks = round((latest["planned"][end] - latest["planned"][start]).days / 7) if latest else None
            setattr(args, key, weeks or DEFAULT_OFFSET_WEEKS[key])
    args.offsets_source = (f"the {latest['version']} release plan" if latest else "defaults")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_table(headers, rows, markdown):
    rows = [["" if c is None else str(c) for c in row] for row in rows]
    if markdown:
        print("| " + " | ".join(headers) + " |")
        print("|" + "|".join("---" for _ in headers) + "|")
        for row in rows:
            print("| " + " | ".join(row) + " |")
        return
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(c.ljust(w) for c, w in zip(row, widths)))


def days(a, b):
    return (b - a).days if a and b else None


def load_history(remote, args, release_dates):
    """
    Release branches to learn from: all since --since if given, otherwise the last --window ones, each
    with its published release plan (if any).
    """
    history = collect_history(remote, args.since, release_dates, None if args.since else args.window)
    plans = fetch_release_plans([h["version"] for h in history],
                                {h["version"]: h["feature_freeze"] for h in history}, args)
    for h in history:
        h["planned"] = plans.get(h["version"])
    resolve_offsets(args, history)
    return history


def cmd_history(args):
    remote = detect_remote(args.remote)
    print_evidence(load_history(remote, args, parse_assignments(args.release_date)), remote, args)


def print_evidence(history, remote, args):
    """
    Prints the past releases the projections are computed from, so that everyone can check the input
    against git instead of having to trust the output.
    """
    print(f"Past releases used for the calculation (last {len(history)} release branches, see --window):")
    print()
    rows = []
    prev = None
    for h in history:
        released = h["released"] or "not yet"
        if h["released_given"]:
            released = f"{released} *"
        rows.append([h["version"], h["feature_freeze"], h["first_rc"], released, h["rc_count"],
                     days(h["feature_freeze"], h["first_rc"]), days(h["first_rc"], h["released"]),
                     days(h["feature_freeze"], h["released"]), h["commits_after_cut"], h["commits_after_first_rc"],
                     days(prev["feature_freeze"], h["feature_freeze"]) if prev else None])
        prev = h
    print_table(["version", "branch cut (FF)", "RC0", "released", "RCs", "cut->RC0 d", "RC0->release d",
                 "cut->release d", "commits after cut", "commits after RC0", "days since prev cut"],
                rows, args.markdown)
    print()
    print(f"Source: git. Branch cut = merge-base of {remote}/X.Y with {remote}/trunk; RC0, RCs and released =")
    print("dates of the X.Y.0-rcN and X.Y.0 tags. Empty RC0 = no rc0 tag kept for that release.")
    if any(h["released_given"] for h in history):
        print("* = release date given with --release-date (not tagged yet).")
    print()
    if any(h["planned"] for h in history):
        print("Planned (wiki release plan) vs actual (git):")
        print()
        rows = []
        for h in history:
            p = h["planned"] or {}
            released = h["released"] or "not yet"
            if h["released_given"]:
                released = f"{released} *"
            rows.append([h["version"], p.get("kip_freeze"), p.get("feature_freeze"), h["feature_freeze"],
                         days(p.get("feature_freeze"), h["feature_freeze"]), p.get("code_freeze"), p.get("target"),
                         released, days(p.get("target"), h["released"])])
        print_table(["version", "KIP freeze", "feature freeze", "branch cut", "slip d", "code freeze",
                     "target release", "released", "slip d"], rows, args.markdown)
        print()
        print('Source: "Release Dates" section of the "Release Plan X.Y.0" pages on cwiki.apache.org, e.g.')
        print(next(h["planned"]["url"] for h in history if h["planned"]))
        print()
    print_stats(history_stats(history, None), args)
    print()


def print_stats(stats, args):
    nominal = (args.code_freeze_weeks + args.target_weeks) * 7
    print(f"Based on {', '.join(stats['versions'])}:")
    for key, label in (("ff_interval", "feature freeze -> next feature freeze"),
                       ("ff_to_rc0", "feature freeze -> first RC"),
                       ("rc0_to_release", "first RC -> release"),
                       ("ff_to_release", "feature freeze -> release"),
                       ("rc_count", "release candidates"),
                       ("planned_ff_to_cut", "planned feature freeze -> branch cut"),
                       ("target_to_release", "planned target -> actual release")):
        values = stats[key]
        if values:
            unit = "" if key == "rc_count" else "d"
            label = f"{label} ({len(values)})"
            print(f"  {label:42} median {percentile(values, 50)}{unit}, p75 {percentile(values, 75)}{unit}, "
                  f"min {min(values)}{unit}, max {max(values)}{unit}")
    print(f"  (release plans assume feature freeze -> release = {nominal}d: "
          f"{args.code_freeze_weeks}w to code freeze + {args.target_weeks}w; phase lengths "
          f"{args.feature_freeze_weeks}w/{args.code_freeze_weeks}w/{args.target_weeks}w from {args.offsets_source})")


def versions_until(start, target):
    """Minor versions after `start` up to and including `target`, or None if `target` is not reachable."""
    versions, version = [], start
    for _ in range(50):
        version = bump_minor(version)
        versions.append(version)
        if version == target:
            return versions
    return None


def parse_scenarios(values):
    """
    Parses repeated --kip-freeze VERSION=DATE arguments. Giving the same version more than once asks for a
    comparison, so this returns one override dict per combination, e.g. two dates for 4.5.0 -> two scenarios.
    """
    by_version = {}
    for value in values or []:
        for version, date in parse_assignments([value]).items():
            by_version.setdefault(version, []).append(date)
    versions = list(by_version)
    return [dict(zip(versions, dates)) for dates in itertools.product(*by_version.values())] or [{}]


def plan_from_args(args, overrides=None):
    """
    Projects the plan described by the command line arguments.

    If --after has no release branch yet (e.g. planning 4.6.0 while 4.5.0 is still in development), the
    releases between the latest release branch and --after are projected too and used as the assumed
    starting point. They are returned separately as `assumed`.
    """
    release_dates = parse_assignments(args.release_date)
    remote = detect_remote(args.remote)
    history = load_history(remote, args, release_dates)
    stats = history_stats(history, None)
    if not stats["ff_to_release"] or not stats["ff_interval"] or not stats["ff_to_rc0"]:
        sys.exit("Not enough released versions in git history, fetch tags or increase --window")

    latest = history[-1]
    if not latest["released"]:
        print(f"Note: {latest['version']} is not released (no tag) yet and is left out of the release duration "
              f"statistics, pass --release-date {latest['version']}=YYYY-MM-DD to include it", file=sys.stderr)
    by_version = {h["version"]: h for h in history}
    args.after = args.after or latest["version"]
    anchor, assumed_versions = args.after, []
    if args.after not in by_version and not args.after_feature_freeze:
        anchor = history[-1]["version"]
        assumed_versions = versions_until(anchor, args.after)
        if assumed_versions is None:
            sys.exit(f"{args.after} is not a later minor release of {anchor} (the latest release branch), "
                     f"pass --after-feature-freeze")
    after = by_version.get(anchor)
    after_ff = parse_date(args.after_feature_freeze) if args.after_feature_freeze else after["feature_freeze"]
    after_release = release_dates.get(anchor) or after and after["released"]

    pct = args.percentile
    versions = assumed_versions + (args.versions.split(",") if args.versions else [])
    count = len(assumed_versions) + args.count
    names, version = [], anchor
    for i in range(count):
        version = versions[i] if i < len(versions) else bump_minor(version)
        names.append(version)
    published = {} if args.ignore_wiki_plan else fetch_release_plans(names, {}, args)
    plan = build_plan(anchor, after_release, after_ff, count,
                      args.feature_freeze_weeks, args.code_freeze_weeks, args.target_weeks,
                      percentile(stats["ff_interval"], 50), percentile(stats["ff_to_rc0"], pct),
                      percentile(stats["ff_to_release"], pct),
                      parse_assignments(args.kip_freeze) if overrides is None else overrides,
                      iso_weeks=[int(w) for w in args.kip_freeze_weeks.split(",")] if args.kip_freeze_weeks
                      else None,
                      versions=names, avoid_holidays=not args.ignore_holidays, published=published)
    assumed = plan[:len(assumed_versions)]
    return argparse.Namespace(stats=stats, history=by_version, history_list=history, remote=remote, anchor=anchor, anchor_ff=after_ff,
                              anchor_release=after_release, assumed=assumed, plan=plan[len(assumed_versions):])


def print_assumed(result):
    for p in result.assumed:
        print(f"Assumed: {p['version']} is not branched yet, using its projected dates "
              f"(feature freeze {p['feature_freeze']}, realistic release {p['realistic']})")


def mail_date(date):
    return f"{date:%b} {date.day}, {date.year}" if date else "-"


def print_mail(result, markdown):
    """
    Prints the plan with the columns used in release discussions on dev@ (KIP Freeze, Feature Freeze, Code
    Freeze, Target Release), preceded by how the previous release was planned and how it actually went.
    """
    rows = []
    previous = result.history.get(result.anchor)
    if previous:
        planned = previous["planned"]
        if planned:
            rows.append([previous["version"], "planned (wiki)"] +
                        [mail_date(planned[k]) for k in ("kip_freeze", "feature_freeze", "code_freeze", "target")] +
                        ["-"])
        released = mail_date(previous["released"]) if previous["released"] else "not released yet"
        # git only knows the branch cut and the release; KIP and code freeze are taken from the plan
        rows.append([previous["version"], "actual (git)", mail_date(planned and planned["kip_freeze"]),
                     mail_date(previous["feature_freeze"]), mail_date(planned and planned["code_freeze"]),
                     released, released])
    for p in result.assumed + result.plan:
        rows.append([p["version"], p["kip_freeze_source"]] + [mail_date(p[k]) for k in (
            "kip_freeze", "feature_freeze", "code_freeze", "nominal", "realistic")])
    print_table(["Version", "Basis", "KIP Freeze", "Feature Freeze", "Code Freeze", "Target Release",
                 "Realistic Release"], rows, markdown)


def print_plan(result, args):
    if args.mail:
        print_mail(result, args.markdown)
    else:
        rows = [p | {"version": f"{p['version']} (assumed)"} for p in result.assumed] + result.plan
        print_table(["version", "KIP freeze", "derived from", "feature freeze", "code freeze", "first RC",
                     "target release", "realistic release"],
                    [[p["version"], p["kip_freeze"], p["kip_freeze_source"], p["feature_freeze"],
                      p["code_freeze"], p["first_rc"], p["nominal"], p["realistic"]] for p in rows], args.markdown)
    warnings = [(p["version"], w) for p in result.assumed + result.plan for w in p["warnings"]]
    if warnings:
        print()
        print("Warnings:")
        for version, warning in warnings:
            print(f"  - {version}: {warning}")


def cmd_plan(args):
    scenarios = parse_scenarios(args.kip_freeze)
    results = [plan_from_args(args, overrides) for overrides in scenarios]
    result = results[0]
    print_evidence(result.history_list, result.remote, args)
    print(f"Latest known release: {result.anchor} (feature freeze {result.anchor_ff}, "
          f"released {result.anchor_release or 'unknown'})")
    print_assumed(result)
    print("Dates: a published wiki release plan is used as is (see --ignore-wiki-plan), otherwise")
    if args.kip_freeze_weeks:
        print(f"  KIP freeze = Wednesday of ISO weeks {args.kip_freeze_weeks}")
    else:
        print(f"  KIP freeze = previous feature freeze + median interval - {args.feature_freeze_weeks}w")
    print(f"  feature freeze = KIP freeze + {args.feature_freeze_weeks}w, code freeze = feature freeze + "
          f"{args.code_freeze_weeks}w, target release = code freeze + {args.target_weeks}w")
    print(f"First RC / realistic release: feature freeze + p{args.percentile} of history")
    print()
    if len(results) == 1:
        print_plan(result, args)
        return

    names = []
    for overrides, scenario in zip(scenarios, results):
        name = ", ".join(f"{v} KIP freeze {mail_date(d)}" for v, d in overrides.items())
        names.append(name)
        print(f"Scenario: {name}")
        print()
        print_plan(scenario, args)
        print()
    print("Comparison:")
    print()
    versions = [p["version"] for p in result.assumed + result.plan]
    headers = ["scenario"] + [f"{v} {k}" for v in versions for k in ("KIP freeze", "realistic")] + ["warnings"]
    rows = []
    for name, scenario in zip(names, results):
        entries = scenario.assumed + scenario.plan
        rows.append([name] + [e[k] for e in entries for k in ("kip_freeze", "realistic")] +
                    [sum(len(e["warnings"]) for e in entries)])
    print_table(headers, rows, args.markdown)


def cmd_status(args):
    today = parse_date(args.today) if args.today else datetime.date.today()
    args.version = args.version or next_release(detect_remote(args.remote))
    major, minor, _ = (int(p) for p in args.version.split("."))
    if minor == 0 and not args.after_feature_freeze:
        sys.exit("Cannot derive the previous release of a major release, pass --after-feature-freeze")
    given_kip_freeze = args.kip_freeze
    args.after, args.count, args.versions = f"{major}.{minor - 1}.0", 1, args.version
    args.kip_freeze = [f"{args.version}={given_kip_freeze}"] if given_kip_freeze else []
    args.kip_freeze_weeks = None
    result = plan_from_args(args)
    print_evidence(result.history_list, result.remote, args)
    print_assumed(result)
    entry = result.plan[0]
    milestones = {"KIP freeze": entry["kip_freeze"], "feature freeze": entry["feature_freeze"],
                  "code freeze": entry["code_freeze"], "first RC": entry["first_rc"],
                  "target release": entry["nominal"], "realistic release": entry["realistic"]}
    source = "given" if given_kip_freeze else entry["kip_freeze_source"]

    branched = result.history.get(args.version)
    if branched:
        # The release branch exists, so what actually happened wins over the projection.
        planned = branched["planned"] or {}
        projected_release = entry["realistic"] - entry["feature_freeze"]
        milestones.update({
            "KIP freeze": planned.get("kip_freeze") or milestones["KIP freeze"],
            "feature freeze": branched["feature_freeze"],
            "code freeze": planned.get("code_freeze") or milestones["code freeze"],
            "first RC": branched["first_rc"] or branched["feature_freeze"] + (entry["first_rc"] -
                                                                              entry["feature_freeze"]),
            "target release": planned.get("target") or milestones["target release"],
            "realistic release": branched["released"] or branched["feature_freeze"] + projected_release,
        })
        source = "release branch cut" + (" and wiki release plan" if planned else "")

    print(f"Release {args.version} as of {today} (dates: {source})")
    for name, date in milestones.items():
        delta = (date - today).days
        when = f"in {delta} days" if delta >= 0 else f"{-delta} days ago"
        print(f"  {name:18} {date}  ({when})")
    for warning in entry["warnings"] if not branched else []:
        print(f"  warning: {warning}")

    if args.no_jira:
        return
    base = f'project = KAFKA AND fixVersion = "{args.version}" AND resolution = Unresolved'
    try:
        total = jira_search(base, max_results=0)["total"]
        blockers = jira_search(f"{base} AND priority in (Blocker, Critical) ORDER BY priority DESC, updated ASC")
    except (OSError, ValueError, http.client.HTTPException) as e:
        print(f"\nCould not query JIRA ({e!r}), rerun with --no-jira to skip")
        return
    print(f"\nOpen JIRA issues with fixVersion {args.version}: {total}, "
          f"of which Blocker/Critical: {blockers['total']}")
    rows = []
    for issue in blockers["issues"]:
        f = issue["fields"]
        updated = parse_date(f["updated"][:10])
        rows.append([issue["key"], f["priority"]["name"], f["status"]["name"],
                     (f.get("assignee") or {}).get("name", "UNASSIGNED"), (today - updated).days,
                     f["summary"][:80]])
    if rows:
        print_table(["key", "priority", "status", "assignee", "days idle", "summary"], rows, args.markdown)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--markdown", action="store_true", help="Print tables as markdown (for the wiki)")
    common.add_argument("--mail", action="store_true",
                        help="Print the plan with the columns and date format used in dev@ release discussions")
    common.add_argument("--feature-freeze-weeks", type=int,
                        help="KIP freeze -> feature freeze (default: from the latest wiki release plan)")
    common.add_argument("--code-freeze-weeks", type=int,
                        help="Feature freeze -> code freeze (default: from the latest wiki release plan)")
    common.add_argument("--target-weeks", type=int,
                        help="Code freeze -> target release (default: from the latest wiki release plan)")

    git_opts = argparse.ArgumentParser(add_help=False)
    git_opts.add_argument("--remote", help="Git remote for apache/kafka (auto-detected by default)")
    git_opts.add_argument("--window", type=int, default=5,
                          help="Learn from the last N release branches (default: 5, about a year and a half)")
    git_opts.add_argument("--since", help="Learn from all release branches since this one instead, e.g. 3.0")
    git_opts.add_argument("--release-date", action="append", metavar="VERSION=DATE",
                          help="Release date for a version not tagged yet, e.g. 4.4.0=2026-10-12")
    git_opts.add_argument("--percentile", type=int, default=50,
                          help="Percentile of historical durations used for first RC and realistic release")
    git_opts.add_argument("--ignore-holidays", action="store_true",
                          help="Do not move derived KIP freezes away from Christmas / Lunar New Year")
    git_opts.add_argument("--no-wiki", action="store_true",
                          help="Do not read release plans from cwiki.apache.org (implies --ignore-wiki-plan)")
    git_opts.add_argument("--ignore-wiki-plan", action="store_true",
                          help="Project upcoming releases from history even if a release plan is published")

    sub = parser.add_subparsers(dest="command")

    history = sub.add_parser("history", parents=[common, git_opts],
                             help="Show how past releases actually went, from git branches and tags")
    history.set_defaults(func=cmd_history)

    plan = sub.add_parser("plan", parents=[common, git_opts], help="Project dates for upcoming releases")
    plan.add_argument("--after",
                      help="Plan the releases after this one, e.g. 4.5.0 (default: latest release branch)")
    plan.add_argument("--after-feature-freeze", help="Feature freeze date of --after if not in git")
    plan.add_argument("--count", type=int, default=3, help="Number of upcoming releases to project")
    plan.add_argument("--kip-freeze-weeks",
                      help="Use the fixed calendar rule instead of history: ISO weeks whose Wednesday is KIP "
                           "freeze, e.g. 9,25,41")
    plan.add_argument("--kip-freeze", action="append", metavar="VERSION=DATE",
                      help="Already agreed KIP freeze to use instead of a computed one, e.g. 4.5.0=2027-01-13. "
                           "Give the same version more than once to compare scenarios")
    plan.add_argument("--versions", help="Comma separated version names, e.g. 4.8.0,4.9.0,5.0.0")
    plan.set_defaults(func=cmd_plan)

    status = sub.add_parser("status", parents=[common, git_opts],
                            help="Countdown to milestones and open blockers of a release")
    status.add_argument("version", nargs="?",
                        help="Release version, e.g. 4.6.0 (default: the next release, which has no branch yet)")
    status.add_argument("--kip-freeze", help="Agreed KIP freeze date (derived from history if omitted)")
    status.add_argument("--after-feature-freeze", help="Feature freeze of the previous release if not in git")
    status.add_argument("--today", help=argparse.SUPPRESS)
    status.add_argument("--no-jira", action="store_true", help="Do not query issues.apache.org")
    status.set_defaults(func=cmd_status)

    argv = list(sys.argv[1:] if argv is None else argv)
    # Options may be given before the command, and "plan" is the default command so that running the
    # script without arguments shows the next releases.
    command = next((a for a in argv if a in sub.choices), None)
    if command:
        argv.remove(command)
        argv.insert(0, command)
    elif not argv or argv[0] not in ("-h", "--help"):
        argv.insert(0, "plan")
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
