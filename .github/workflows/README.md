# GitHub Actions

## Overview

The entry point for our build is the "CI" workflow which is defined in ci.yml.
This is used for both PR and trunk builds. The jobs and steps of the workflow
are defined in build.yml.

For Pull Requests, the "CI" workflow runs in an unprivileged context. This means
it does not have access to repository secrets. After the "CI" workflow is complete, 
the "CI Complete" workflow is automatically run. This workflow consumes artifacts
from the "CI" workflow and does run in a privileged context. This is how we are
able to upload Gradle Build Scans to Develocity without exposing our access
token to the Pull Requests.

## Disabling Email Notifications

By default, GitHub sends an email for each failed action run. To change this,
visit https://github.com/settings/notifications and find System -> Actions.
Here you can change your notification preferences.

## GitHub Actions Quirks

### Composite Actions

Composite actions are a convenient way to reuse build logic, but they have
some limitations. 

- Cannot run more than one step in a composite action (see `workflow_call` instead)
- Inputs can only be strings, no support for typed parameters. See: https://github.com/actions/runner/issues/2238