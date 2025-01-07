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

## Our Workflows

### Trunk Build

The [ci.yml](ci.yml) is run when commits are pushed to trunk. This calls into [build.yml](build.yml)
to run our main build. In the trunk build, we do not read from the Gradle cache,
but we do write to it. Also, the test catalog is only updated from trunk builds.

### PR Build

Similar to trunk, this workflow starts in [ci.yml](ci.yml) and calls into [build.yml](build.yml).
Unlike trunk, the PR builds _will_ utilize the Gradle cache.

### PR Triage

In order to get the attention of committers, we have a triage workflow for Pull Requests
opened by non-committers. This workflow consists of two files:

* [pr-update.yml](pr-update.yml) When a PR is created, add the `triage` label if the PR
  was opened by a non-committer.
* [pr-reviewed.yml](pr-reviewed.yml) Cron job to remove the `triage` label from PRs which have been reviewed

_The pr-update.yml workflow includes pull_request_target!_

For committers to avoid having this label added, their membership in the ASF GitHub
organization must be public. Here are the steps to take:

* Navigate to the ASF organization's "People" page https://github.com/orgs/apache/people
* Find yourself
* Change "Organization Visibility" to Public

Full documentation for this process can be found in GitHub's docs: https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-your-membership-in-organizations/publicizing-or-hiding-organization-membership

If you are a committer and do not want your membership in the ASF org listed as public, 
you will need to remove the `triage` label manually.

### CI Approved

Due to a combination of GitHub security and ASF's policy, we required explicit
approval of workflows on PRs submitted by non-committers (and non-contributors).
To simply this process, we have a `ci-approved` label which automatically approves
these workflows.

There are two files related to this workflow:

* [pr-labeled.yml](pr-labeled.yml) approves a pending approval for PRs that have
been labeled with `ci-approved`
* [ci-requested.yml](ci-requested.yml) approves future workflow requests automatically
if the PR has the `ci-approved` label

_The pr-labeled.yml workflow includes pull_request_target!_

### Stale PRs

This one is straightforward. Using the "actions/stale" GitHub Action, we automatically
label and eventually close PRs which have not had activity for some time. See the
[stale.yml](stale.yml) workflow file for specifics.

## GitHub Actions Quirks

### Composite Actions

Composite actions are a convenient way to reuse build logic, but they have
some limitations. 

- Cannot run more than one step in a composite action (see `workflow_call` instead)
- Inputs can only be strings, no support for typed parameters. See: https://github.com/actions/runner/issues/2238