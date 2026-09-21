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
