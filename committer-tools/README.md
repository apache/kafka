# Committer Tools

This directory contains scripts to help Apache Kafka committers with a few chores.
Some of the scripts require a GitHub API token with write permissions. Only
committers will be able to utilize such scripts.

## Requirements

- Python 3.x and pip
- The GitHub CLI

## Installation

### 1. Check Python installation

Check if Python and pip are installed in your system.

```bash
python3 --version
pip3 --version
```

### 2. Set up a virtual environment (optional)

```bash
python3 -m venv venv

# For Linux/macOS
source venv/bin/activate

# On Windows:
# .\venv\Scripts\activate
```

### 3. Install the required dependencies

```bash
pip3 install -r requirements.txt
```

### 4. Install the GitHub CLI

See: https://cli.github.com/

```bash
brew install gh
```

## Find Reviewers

The reviewers.py script is used to simplify the process of producing our "Reviewers:"
Git trailer. It parses the Git log to gather a set of "Authors" and "Reviewers". 
Some simple string prefix matching is done to find candidates.

Usage:

```bash
python3 reviewers.py
```

## Refresh Collaborators

The Refresh Collaborators script automates the process of fetching contributor
data from GitHub repositories, filtering top contributors who are not part of
the existing committers, and updating a local configuration file (.asf.yaml) to
include these new contributors.

> This script requires the Python dependencies and a GitHub auth token.

You need to set up a valid GitHub token to access the repository. After you
generate it (or authenticate via GitHub CLI), this can be done by setting the
GITHUB_TOKEN environment variable.

```bash
# For Linux/macOS
export GITHUB_TOKEN="your_github_token"
# Or if you use GitHub CLI
export GITHUB_TOKEN="$(gh auth token)"

# On Windows:
# .\venv\Scripts\activate
```

Usage:

```bash
python3 refresh_collaborators.py
```

## Approve GitHub Action Workflows

This script allows a committer to approve GitHub Action workflow runs from 
non-committers. It fetches the latest 20 workflow runs that are in the 
`action_required` state and prompts the user to approve the run.

> This script requires the `gh` tool

Usage:

```bash
python3 approve-workflows.py
```
