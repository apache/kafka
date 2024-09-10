# Refresh Collaborators Script

The Refresh Collaborators script automates the process of fetching contributor
data from GitHub repositories, filtering top contributors who are not part of
the existing committers, and updating a local configuration file (.asf.yaml) to
include these new contributors.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)

## Requirements

- Python 3.x and pip
- A valid GitHub token with repository read access

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

3. Install the required dependencies

```bash
pip3 install -r requirements.txt
```

## Usage

### 1. Set up the environment variable for GitHub Token

You need to set up a valid GitHub token to access the repository. After you
generate it, this can be done by setting the GITHUB_TOKEN environment variable.

```bash
# For Linux/macOS
export GITHUB_TOKEN="your_github_token"

# On Windows:
# .\venv\Scripts\activate
```

### 2. Run the script

```bash
python3 refresh_collaborators.py
```
