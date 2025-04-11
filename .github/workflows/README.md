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

* [pr-update.yml](pr-update.yml) When a PR is created, add the `triage` label if 
  the PR was opened by a non-committer.
* [pr-labels-cron.yml](pr-labels-cron.yml) Cron job to add `needs-attention` label to community 
  PRs that have not been reviewed after 7 days. Also includes a cron job to 
  remove the `triage` and `needs-attention` labels from PRs which have been reviewed. 

_The pr-update.yml workflow includes pull_request_target!_

For committers to avoid having this label added, their membership in the ASF GitHub
organization must be public. Here are the steps to take:

* Navigate to the ASF organization's "People" page https://github.com/orgs/apache/people
* Find yourself
* Change "Organization Visibility" to Public

Full documentation for this process can be found in GitHub's docs: 
https://docs.github.com/en/account-and-profile/setting-up-and-managing-your-personal-account-on-github/managing-your-membership-in-organizations/publicizing-or-hiding-organization-membership

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
* [workflow-requested.yml](workflow-requested.yml) approves future workflow requests automatically
if the PR has the `ci-approved` label

_The pr-labeled.yml workflow includes pull_request_target!_

### PR Linter

To help ensure good commit messages, we have added a "Pull Request Linter" job
that checks the title and body of the PR. 

There are two files related to this workflow:

* [pr-reviewed.yml](pr-reviewed.yml) runs when a PR is reviewed or has its title
or body edited. This workflow simply captures the PR number into a text file
* [pr-linter.yml](pr-linter.yml) runs after pr-reviewed.yml and loads the PR
using the saved text file. This workflow runs the linter script that checks the
structure of the PR

Note that the pr-reviewed.yml workflow uses the `ci-approved` mechanism described
above.

The following checks are performed on our PRs:
* Title is not too short or too long
* Title starts with "KAFKA-", "MINOR", or "HOTFIX"
* Body is not empty
* Body includes "Reviewers:" if the PR is approved

With the merge queue, our PR title and body will become the commit subject and message.
This linting step will help to ensure that we have nice looking commits.

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

## Troubleshooting

### Gradle Cache Misses

If your PR is running for longer than you would expect due to cache misses, there are a
few things to check. 

First, find the cache that was loaded into your PR build. This is found in the Setup Gradle
output. Look for a line starting with "Restored Gradle User Home from cache key". 
For example,

```
Restored Gradle User Home from cache key: gradle-home-v1|Linux-X64|test[188616818c9a3165053ef8704c27b28e]-5c20aa187aa8f51af4270d7d1b0db4963b0cd10b
```

The last part of the cache key is the SHA of the commit on trunk where the cache
was created. If that commit is not on your branch, it means your build loaded a 
cache that includes changes your PR does not yet have. This is a common way to
have cache misses. To resolve this, update your PR with the latest cached trunk commit:

```commandline
git fetch origin
./committer-tools/update-cache.sh
git merge trunk-cached
```

then push your branch.

If your build seems to be using the correct cache, the next thing to check is for
changes to task inputs. You can find this by locating the trunk Build Scan from 
the cache commit on trunk and comparing it with the build scan of your PR build.
This is done in the Develocity UI using the two overlapping circles like `(A()B)`. 
This will show you differences in the task inputs for the two builds.

Finally, you can run your PR with extra cache debugging. Add this to the gradle invocation in
[run-gradle/action.yml](../actions/run-gradle/action.yml). 

```
-Dorg.gradle.caching.debug=true
```

This will dump out a lot of output, so you may also reduce the test target to one module.
