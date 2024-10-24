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

## Security

Please read the following GitHub articles before authoring new workflows.

1) https://github.blog/security/supply-chain-security/four-tips-to-keep-your-github-actions-workflows-secure/
2) https://securitylab.github.com/resources/github-actions-preventing-pwn-requests/

### Variable Injection

Any workflows that use the `run` directive should avoid using the `${{ ... }}` syntax.
Instead, declare all injectable variables as environment variables. For example:

```yaml
    - name: Copy RC Image to promoted image
      env:
        PROMOTED_DOCKER_IMAGE: ${{ github.event.inputs.promoted_docker_image }}
        RC_DOCKER_IMAGE: ${{ github.event.inputs.rc_docker_image }}
      run: |
        docker buildx imagetools create --tag $PROMOTED_DOCKER_IMAGE $RC_DOCKER_IMAGE
```

This prevents untrusted inputs from doing script injection in the `run` steps.

### `pull_request_target` events

In addition to the above security articles, please review the [official documentation](https://docs.github.com/en/actions/writing-workflows/choosing-when-your-workflow-runs/events-that-trigger-workflows#pull_request_target)
on `pull_request_target`. This event type allows PRs to trigger actions that run
with elevated permission and access to repository secrets. We should only be 
using this for very simple tasks such as applying labels or adding comments to PRs.

_We must never run the untrusted PR code in the elevated `pull_request_target` context_

## GitHub Actions Quirks

### Composite Actions

Composite actions are a convenient way to reuse build logic, but they have
some limitations. 

- Cannot run more than one step in a composite action (see `workflow_call` instead)
- Inputs can only be strings, no support for typed parameters. See: https://github.com/actions/runner/issues/2238