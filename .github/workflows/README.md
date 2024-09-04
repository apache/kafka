# GitHub Actions

## Overview

The entry point for our build is the "CI" workflow which is defined in ci.yml.
This is used for both PR and trunk builds. The jobs and steps of the workflow
are defined in build.yml.

## Opting-in to GitHub Actions

To opt-in to the new GitHub actions workflows, simply name your branch with a
prefix of "gh-". For example, `gh-KAFKA-17433-deflake`

## Disabling Email Notifications

By default, GitHub sends an email for each failed action run. To change this,
visit https://github.com/settings/notifications and find System -> Actions.
Here you can change your notification preferences.

## Publishing Build Scans

> This only works for committers (who have ASF accounts on ge.apache.org).

There are two ways committers can have build scans published. The simplest
way is to push their branches to apache/kafka. This will allow GitHub Actions to
have access to the repository secret needed to publish the scan.

Alternatively, committers create pull requests against their own forks and
configure their own access key as a repository secret.

Log in to https://ge.apache.org/, click on My Settings and then Access Keys

Generate an Access Key and give it a name like "github-actions". Copy the key
down somewhere safe.

On your fork of apache/kafka, navigate to Settings -> Security -> Secrets and
Variables -> Actions. In the Secrets tab, click Create a New Repository Secret.
The name of the secret should be `GE_ACCESS_TOKEN` and the value should
be `ge.apache.org=abc123` where "abc123" is substituted for the Access Key you
previously generated.
