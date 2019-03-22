#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing them to Apache. This script is a modified version
# of the one created by the Spark project (https://github.com/apache/spark/blob/master/dev/merge_spark_pr.py).
#
# Usage: ./kafka-merge-pr.py (see config env vars below)
#
# This utility assumes you already have local a kafka git folder and that you
# have added remotes corresponding to both:
# (i) the github apache kafka mirror and
# (ii) the apache kafka git repo.

import json
import os
import re
import subprocess
import sys
import urllib2

try:
    import jira.client
    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

PROJECT_NAME = "kafka"

CAPITALIZED_PROJECT_NAME = "kafka".upper()

# Location of the local git repository
REPO_HOME = os.environ.get("%s_HOME" % CAPITALIZED_PROJECT_NAME, os.getcwd())
# Remote name which points to the GitHub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name where we want to push the changes to (GitHub by default, but Apache Git would work if GitHub is down)
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache-github")
# ASF JIRA username
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "")
# ASF JIRA password
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "")
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")

GITHUB_USER = os.environ.get("GITHUB_USER", "apache")
GITHUB_BASE = "https://github.com/%s/%s/pull" % (GITHUB_USER, PROJECT_NAME)
GITHUB_API_BASE = "https://api.github.com/repos/%s/%s" % (GITHUB_USER, PROJECT_NAME)
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
TEMP_BRANCH_PREFIX = "PR_TOOL"

DEV_BRANCH_NAME = "trunk"

DEFAULT_FIX_VERSION = os.environ.get("DEFAULT_FIX_VERSION", "2.2.1")

def get_json(url):
    try:
        request = urllib2.Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header('Authorization', 'token %s' % GITHUB_OAUTH_KEY)
        return json.load(urllib2.urlopen(request))
    except urllib2.HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == '0':
            print "Exceeded the GitHub API rate limit; see the instructions in " + \
                  "kafka-merge-pr.py to configure an OAuth token for making authenticated " + \
                  "GitHub requests."
        else:
            print "Unable to fetch URL, exiting: %s" % url
        sys.exit(-1)


def fail(msg):
    print msg
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print cmd
    if isinstance(cmd, list):
        return subprocess.check_output(cmd)
    else:
        return subprocess.check_output(cmd.split(" "))


def continue_maybe(prompt):
    result = raw_input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")

def clean_up():
    if original_head != get_current_branch():
        print "Restoring head pointer to %s" % original_head
        run_cmd("git checkout %s" % original_head)

    branches = run_cmd("git branch").replace(" ", "").split("\n")

    for branch in filter(lambda x: x.startswith(TEMP_BRANCH_PREFIX), branches):
        print "Deleting local branch %s" % branch
        run_cmd("git branch -D %s" % branch)

def get_current_branch():
    return run_cmd("git rev-parse --abbrev-ref HEAD").replace("\n", "")

# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc):
    pr_branch_name = "%s_MERGE_PR_%s" % (TEMP_BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (TEMP_BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(['git', 'merge', pr_branch_name, '--squash'])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    commit_authors = run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name,
                             '--pretty=format:%an <%ae>']).split("\n")
    distinct_authors = sorted(set(commit_authors),
                              key=lambda x: commit_authors.count(x), reverse=True)
    primary_author = raw_input(
        "Enter primary author in the format of \"name <email>\" [%s]: " %
        distinct_authors[0])
    if primary_author == "":
        primary_author = distinct_authors[0]

    reviewers = raw_input(
        "Enter reviewers in the format of \"name1 <email1>, name2 <email2>\": ").strip()

    run_cmd(['git', 'log', 'HEAD..%s' % pr_branch_name, '--pretty=format:%h [%an] %s']).split("\n")

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    
    if body is not None:
        # Remove "Committer Checklist" section
        checklist_index = body.find("### Committer Checklist")
        if checklist_index != -1:
            body = body[:checklist_index].rstrip()
        # Remove @ symbols from the body to avoid triggering e-mails to people every time someone creates a
        # public fork of the project.
        body = body.replace("@", "")
        merge_message_flags += ["-m", body]

    authors = "\n".join(["Author: %s" % a for a in distinct_authors])

    merge_message_flags += ["-m", authors]

    if reviewers != "":
        merge_message_flags += ["-m", "Reviewers: %s" % reviewers]

    if had_conflicts:
        committer_name = run_cmd("git config --get user.name").strip()
        committer_email = run_cmd("git config --get user.email").strip()
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name, committer_email)
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    close_line = "Closes #%s from %s" % (pr_num, pr_repo_desc)
    merge_message_flags += ["-m", close_line]

    run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe("Merge complete (local ref %s). Push to %s?" % (
        target_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def cherry_pick(pr_num, merge_hash, default_branch):
    pick_ref = raw_input("Enter a branch name [%s]: " % default_branch)
    if pick_ref == "":
        pick_ref = default_branch

    pick_branch_name = "%s_PICK_PR_%s_%s" % (TEMP_BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)

    try:
        run_cmd("git cherry-pick -sx %s" % merge_hash)
    except Exception as e:
        msg = "Error cherry-picking: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and finish the cherry-pick. Finished?"
        continue_maybe(msg)

    continue_maybe("Pick complete (local ref %s). Push to %s?" % (
        pick_branch_name, PUSH_REMOTE_NAME))

    try:
        run_cmd('git push %s %s:%s' % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released versions
    if branch == DEV_BRANCH_NAME:
        versions = filter(lambda x: x == DEFAULT_FIX_VERSION, versions)
        if len(versions) > 0:
            return versions[0]
        else:
            return None
    else:
        versions = filter(lambda x: x.startswith(branch), versions)
        if len(versions) > 0:
            return versions[-1]
        else:
            return None


def resolve_jira_issue(merge_branches, comment, default_jira_id=""):
    asf_jira = jira.client.JIRA({'server': JIRA_API_BASE},
                                basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

    jira_id = raw_input("Enter a JIRA id [%s]: " % default_jira_id)
    if jira_id == "":
        jira_id = default_jira_id

    try:
        issue = asf_jira.issue(jira_id)
    except Exception as e:
        fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    cur_status = issue.fields.status.name
    cur_summary = issue.fields.summary
    cur_assignee = issue.fields.assignee
    if cur_assignee is None:
        cur_assignee = "NOT ASSIGNED!!!"
    else:
        cur_assignee = cur_assignee.displayName

    if cur_status == "Resolved" or cur_status == "Closed":
        fail("JIRA issue %s already has status '%s'" % (jira_id, cur_status))
    print ("=== JIRA %s ===" % jira_id)
    print ("summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n" % (
        cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id))

    versions = asf_jira.project_versions(CAPITALIZED_PROJECT_NAME)
    versions = sorted(versions, key=lambda x: x.name, reverse=True)
    versions = filter(lambda x: x.raw['released'] is False, versions)

    version_names = map(lambda x: x.name, versions)
    default_fix_versions = map(lambda x: fix_version_from_branch(x, version_names), merge_branches)
    default_fix_versions = filter(lambda x: x != None, default_fix_versions)
    default_fix_versions = ",".join(default_fix_versions)

    fix_versions = raw_input("Enter comma-separated fix version(s) [%s]: " % default_fix_versions)
    if fix_versions == "":
        fix_versions = default_fix_versions
    fix_versions = fix_versions.replace(" ", "").split(",")

    def get_version_json(version_str):
        return filter(lambda v: v.name == version_str, versions)[0].raw

    jira_fix_versions = map(lambda v: get_version_json(v), fix_versions)

    resolve = filter(lambda a: a['name'] == "Resolve Issue", asf_jira.transitions(jira_id))[0]
    resolution = filter(lambda r: r.raw['name'] == "Fixed", asf_jira.resolutions())[0]
    asf_jira.transition_issue(
        jira_id, resolve["id"], fixVersions = jira_fix_versions,
        comment = comment, resolution = {'id': resolution.raw['id']})

    print "Successfully resolved %s with fixVersions=%s!" % (jira_id, fix_versions)


def resolve_jira_issues(title, merge_branches, comment):
    jira_ids = re.findall("%s-[0-9]{4,5}" % CAPITALIZED_PROJECT_NAME, title)

    if len(jira_ids) == 0:
        resolve_jira_issue(merge_branches, comment)
    for jira_id in jira_ids:
        resolve_jira_issue(merge_branches, comment, jira_id)


def standardize_jira_ref(text):
    """
    Standardize the jira reference commit message prefix to "PROJECT_NAME-XXX; Issue"

    >>> standardize_jira_ref("%s-5954; Top by key" % CAPITALIZED_PROJECT_NAME)
    'KAFKA-5954; Top by key'
    >>> standardize_jira_ref("%s-5821; ParquetRelation2 CTAS should check if delete is successful" % PROJECT_NAME)
    'KAFKA-5821; ParquetRelation2 CTAS should check if delete is successful'
    >>> standardize_jira_ref("%s-4123 [WIP] Show new dependencies added in pull requests" % PROJECT_NAME)
    'KAFKA-4123; [WIP] Show new dependencies added in pull requests'
    >>> standardize_jira_ref("%s  5954: Top by key" % PROJECT_NAME)
    'KAFKA-5954; Top by key'
    >>> standardize_jira_ref("%s-979 a LRU scheduler for load balancing in TaskSchedulerImpl" % PROJECT_NAME)
    'KAFKA-979; a LRU scheduler for load balancing in TaskSchedulerImpl'
    >>> standardize_jira_ref("%s-1094 Support MiMa for reporting binary compatibility across versions." % CAPITALIZED_PROJECT_NAME)
    'KAFKA-1094; Support MiMa for reporting binary compatibility across versions.'
    >>> standardize_jira_ref("[WIP] %s-1146; Vagrant support" % CAPITALIZED_PROJECT_NAME)
    'KAFKA-1146; [WIP] Vagrant support'
    >>> standardize_jira_ref("%s-1032. If Yarn app fails before registering, app master stays aroun..." % PROJECT_NAME)
    'KAFKA-1032; If Yarn app fails before registering, app master stays aroun...'
    >>> standardize_jira_ref("%s-6250 %s-6146 %s-5911: Types are now reserved words in DDL parser." % (PROJECT_NAME, PROJECT_NAME, CAPITALIZED_PROJECT_NAME))
    'KAFKA-6250 KAFKA-6146 KAFKA-5911; Types are now reserved words in DDL parser.'
    >>> standardize_jira_ref("Additional information for users building from source code")
    'Additional information for users building from source code'
    """
    jira_refs = []
    components = []

    # Extract JIRA ref(s):
    pattern = re.compile(r'(%s[-\s]*[0-9]{3,6})+' % CAPITALIZED_PROJECT_NAME, re.IGNORECASE)
    for ref in pattern.findall(text):
        # Add brackets, replace spaces with a dash, & convert to uppercase
        jira_refs.append(re.sub(r'\s+', '-', ref.upper()))
        text = text.replace(ref, '')

    # Extract project name component(s):
    # Look for alphanumeric chars, spaces, dashes, periods, and/or commas
    pattern = re.compile(r'(\[[\w\s,-\.]+\])', re.IGNORECASE)
    for component in pattern.findall(text):
        components.append(component.upper())
        text = text.replace(component, '')

    # Cleanup any remaining symbols:
    pattern = re.compile(r'^\W+(.*)', re.IGNORECASE)
    if (pattern.search(text) is not None):
        text = pattern.search(text).groups()[0]

    # Assemble full text (JIRA ref(s), module(s), remaining text)
    jira_prefix = ' '.join(jira_refs).strip()
    if jira_prefix:
        jira_prefix = jira_prefix + "; "
    clean_text = jira_prefix + ' '.join(components).strip() + " " + text.strip()

    # Replace multiple spaces with a single space, e.g. if no jira refs and/or components were included
    clean_text = re.sub(r'\s+', ' ', clean_text.strip())

    return clean_text

def main():
    global original_head

    original_head = get_current_branch()

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = filter(lambda x: x[0].isdigit(), [x['name'] for x in branches])
    # Assumes branch names can be sorted lexicographically
    latest_branch = sorted(branch_names, reverse=True)[0]

    pr_num = raw_input("Which pull request would you like to merge? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]

    pr_title = pr["title"]
    commit_title = raw_input("Commit title [%s]: " % pr_title.encode("utf-8")).decode("utf-8")
    if commit_title == "":
        commit_title = pr_title

    # Decide whether to use the modified title or not
    modified_title = standardize_jira_ref(commit_title)
    if modified_title != commit_title:
        print "I've re-written the title as follows to match the standard format:"
        print "Original: %s" % commit_title
        print "Modified: %s" % modified_title
        result = raw_input("Would you like to use the modified title? (y/n): ")
        if result.lower() == "y":
            commit_title = modified_title
            print "Using modified title:"
        else:
            print "Using original title:"
        print commit_title

    body = pr["body"]
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    # Merged pull requests don't appear as merged in the GitHub API;
    # Instead, they're closed by asfgit.
    merge_commits = \
        [e for e in pr_events if e["actor"]["login"] == "asfgit" and e["event"] == "closed"]

    if merge_commits:
        merge_hash = merge_commits[0]["commit_id"]
        message = get_json("%s/commits/%s" % (GITHUB_API_BASE, merge_hash))["commit"]["message"]

        print "Pull request %s has already been merged, assuming you want to backport" % pr_num
        commit_is_downloaded = run_cmd(['git', 'rev-parse', '--quiet', '--verify',
                                    "%s^{commit}" % merge_hash]).strip() != ""
        if not commit_is_downloaded:
            fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

        print "Found commit %s:\n%s" % (merge_hash, message)
        cherry_pick(pr_num, merge_hash, latest_branch)
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = "Pull request %s is not mergeable in its current form.\n" % pr_num + \
            "Continue? (experts only!)"
        continue_maybe(msg)

    print ("\n=== Pull Request #%s ===" % pr_num)
    print ("PR title\t%s\nCommit title\t%s\nSource\t\t%s\nTarget\t\t%s\nURL\t\t%s" % (
        pr_title, commit_title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(pr_num, target_ref, commit_title, body, pr_repo_desc)

    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    while raw_input("\n%s (y/n): " % pick_prompt).lower() == "y":
        merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, latest_branch)]

    if JIRA_IMPORTED:
        if JIRA_USERNAME and JIRA_PASSWORD:
            continue_maybe("Would you like to update an associated JIRA?")
            jira_comment = "Issue resolved by pull request %s\n[%s/%s]" % (pr_num, GITHUB_BASE, pr_num)
            resolve_jira_issues(commit_title, merged_refs, jira_comment)
        else:
            print "JIRA_USERNAME and JIRA_PASSWORD not set"
            print "Exiting without trying to close the associated JIRA."
    else:
        print "Could not find jira-python library. Run 'sudo pip install jira' to install."
        print "Exiting without trying to close the associated JIRA."

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if (failure_count):
        exit(-1)

    main()
