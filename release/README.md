Releasing Apache Kafka
======================

This directory contains the tools used to publish a release.

# Requirements

* python 3.12
* git
* gpg 2.4
* sftp


# Setup

Create a virtualenv for python, activate it and install dependencies:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Usage

To start a release, first activate the virutalenv, and then run
the release script.

```
source .venv/bin/activate
python release.py
```

