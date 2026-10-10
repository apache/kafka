Releasing Apache Kafka
======================

This directory contains the tools used to publish a release.

# Requirements

* python 3.12
* git
* gpg 2.4

The full instructions for producing a release are available in
https://cwiki.apache.org/confluence/display/KAFKA/Release+Process.


# Setup

Create a virtualenv for python, activate it and install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Usage

To start a release, first activate the virtualenv, and then run
the release script.

```
source .venv/bin/activate
```

You'll need to setup `PUSH_REMOTE_NAME` to refer to
the git remote for `apache/kafka`.

```
export PUSH_REMOTE_NAME=<value>
```

It should be the value shown with this command:

```
git remote -v | grep -w 'github.com' | grep -w 'apache/kafka' | grep -w '(push)' | awk '{print $1}'
```

Then start the release script:

```
python release.py
```

Should you encounter some problem, where re-running the script doesn't work, look at the following steps:

- The script remembers data inputted previously if you need to correct it, it is saved under the
`.release-settings.json` file in the `release` folder.
- If the script is interrupted you might need to manually delete the tag named after the release candidate name and
branch named after the release version.

# Docker workflow triggers

After the RC tag is pushed, the script triggers the Docker image build/test and
RC release workflows on GitHub Actions.

## GitHub Personal Access Token

Triggering the workflows requires a GitHub Personal Access Token. To generate one:

1. Go to https://github.com/settings/tokens
2. Click "Generate new token" → "Generate new token (classic)"
3. Set a name (e.g. `kafka-release`)
4. Set an expiration (7 days is sufficient for a release cycle)
5. Select the `repo` scope (this includes the `actions` write permission)
6. Click "Generate token" and copy the token (starts with `ghp_...`)

The token is cached in `.release-settings.json` so it only needs to be entered
once per release cycle. To reset the saved token, remove the `github_token`
entry from `.release-settings.json` or delete the file entirely.

## Optional environment variables

- `GITHUB_REPO`: target repository for the workflow dispatches. Defaults to
  `apache/kafka`. Set this to your fork (e.g. `myuser/kafka`) when testing the
  release script end-to-end without affecting `apache/kafka`.
- `GITHUB_DRY_RUN`: when set to `true`, prints the GitHub API calls that would
  be made instead of executing them. Useful for verifying the flow without a
  token or network access.

```
GITHUB_DRY_RUN=true GITHUB_REPO=myuser/kafka python release.py
```

# Release schedule helper

`schedule.py` helps release managers and committers agree on release dates from data instead of
re-discussing them on every release. It only needs python 3 and git (no virtualenv), plus up-to-date
release branches and tags from `apache/kafka` (`git fetch <remote> --tags`). By default it learns from
the last 5 release branches (`--window`), or from all branches since a given one (`--since`).

Show how past releases actually went: feature freeze (release branch cut), first RC, release date,
number of RCs, commits landed on the release branch after the cut / after RC0, and the medians:

```
python schedule.py history --release-date 4.4.0=2026-10-12
```

Project the next releases (`--count`, default 3) after the latest release branch. This is the
default command, so running the script without arguments is enough. Without further input, every date is derived from history: the
next feature freeze is the previous one plus the typical interval between feature freezes, KIP freeze
is 3 weeks before and code freeze 2 weeks after it, and the KIP freeze is moved later while a
milestone falls on Christmas or Lunar New Year. Next to the nominal release date (code freeze + 4
weeks) it prints a realistic one (feature freeze + typical feature freeze to release duration) and
warns about overlapping releases and very short cycles:

```
python schedule.py
```

Until the latest release is tagged, add its release date to include it in the statistics, e.g.
`--release-date 4.4.0=2026-10-12`. Use `--after 4.5.0` to plan from a later release; releases that
have no branch yet are projected first and marked as assumed.

Use `--kip-freeze 4.5.0=2027-01-13` for a date already agreed on dev@, `--kip-freeze-weeks 9,25,41`
to apply a fixed calendar rule instead of history, and `--percentile 75` for more conservative dates.

Show the milestone countdown and open Blocker/Critical JIRA issues for the next release, or for a
given one (milestones are derived from history unless `--kip-freeze` is given):

```
python schedule.py status
python schedule.py status 4.6.0
```

The planned KIP freeze, feature freeze, code freeze and target release dates are read from the
"Release Dates" section of the "Release Plan X.Y.0" pages on cwiki.apache.org and shown next to what
actually happened. The phase lengths (KIP freeze -> feature freeze -> code freeze -> target release)
default to those of the most recent published plan. If an upcoming release already has a published
plan, its dates are used as is and only the realistic dates are projected; `--ignore-wiki-plan` shows
the pure history-based projection instead, and `--no-wiki` skips reading the wiki altogether.

To answer "would a week earlier be better?" style questions, give `--kip-freeze` more than once for
the same version; every option is projected and compared side by side. `--mail` prints the plan in
a table with the KIP Freeze / Feature Freeze / Code Freeze / Target Release columns and date format used on dev@ (add `--markdown` for a markdown table):

```
python schedule.py --kip-freeze 4.5.0=2027-01-13 --kip-freeze 4.5.0=2027-01-06 --kip-freeze-weeks 9,25,41 --mail
```

Add `--markdown` to any command to get tables that can be pasted into the release plan wiki page or
a mail to dev@.
