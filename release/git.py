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

"""
Auxiliary function to interact with git.
"""

import os

from runtime import repo_dir, execute, cmd

push_remote_name = os.environ.get("PUSH_REMOTE_NAME", "apache-github")


def __defaults(kwargs):
    if "cwd" not in kwargs:
        kwargs["cwd"] = repo_dir


def ensure_no_staged_changes(**kwargs):
    __defaults(kwargs)
    execute("git diff --cached --exit-code --quiet", **kwargs)
    return True


def ensure_no_unstaged_changes(**kwargs):
    __defaults(kwargs)
    execute("git diff --exit-code --quiet", **kwargs)
    return True


def fetch_tags(remote=push_remote_name, **kwargs):
    __defaults(kwargs)
    cmd(f"Fetching tags from {remote}", f"git fetch --tags {remote}", **kwargs)


def tags(**kwargs):
    __defaults(kwargs)
    return execute("git tag", **kwargs).split()


def tag_exists(tag, **kwargs):
    __defaults(kwargs)
    return tag in tags(**kwargs)


def delete_tag(tag, **kwargs):
    __defaults(kwargs)
    if tag_exists(tag, **kwargs):
        execute(f"git tag -d {tag}", **kwargs)


def current_branch(**kwargs):
    __defaults(kwargs)
    return execute("git rev-parse --abbrev-ref HEAD", **kwargs)


def reset_hard_head(**kwargs):
    __defaults(kwargs)
    cmd("Resetting branch", "git reset --hard HEAD", **kwargs)


def contributors(from_rev, to_rev, **kwargs):
    __defaults(kwargs)
    kwargs["shell"] = True
    line = "git shortlog -sn --group=author --group=trailer:co-authored-by"
    line += f" --group=trailer:Reviewers --no-merges {from_rev}..{to_rev}"
    line += " | cut -f2 | sort --ignore-case | uniq"
    return [str(x) for x in filter(None, execute(line, **kwargs).split('\n'))]

def branches(**kwargs):
    output = execute('git branch')
    return [line.replace('*', ' ').strip() for line in output.splitlines()]


def branch_exists(branch, **kwargs):
    __defaults(kwargs)
    return branch in branches(**kwargs)


def delete_branch(branch, **kwargs):
    __defaults(kwargs)
    if branch_exists(branch, **kwargs):
        cmd(f"Deleting git branch {branch}", f"git branch -D {branch}", **kwargs)


def switch_branch(branch, **kwargs):
    __defaults(kwargs)
    execute(f"git checkout {branch}", **kwargs)


def create_branch(branch, ref, **kwargs):
    __defaults(kwargs)
    cmd(f"Creating git branch {branch} to track {ref}", f"git checkout -b {branch} {ref}", **kwargs)


def clone(url, target, **kwargs):
    __defaults(kwargs)
    execute(f"git clone {url} {target}", **kwargs)


def targz(rev, prefix, target, **kwargs):
    __defaults(kwargs)
    line = "git archive --format tar.gz"
    line += f" --prefix {prefix} --output {target} {rev}"
    cmd(f"Creating targz {target} from git rev {rev}", line, **kwargs)


def commit(message, **kwargs):
    __defaults(kwargs)
    cmd("Committing git changes", ["git", "commit", "-a", "-m", message], **kwargs)


def create_tag(tag, **kwargs):
    __defaults(kwargs)
    cmd(f"Creating git tag {tag}", ["git", "tag", "-a", tag, "-m", tag], **kwargs)


def push_ref(ref, remote=push_remote_name, **kwargs):
    __defaults(kwargs)
    cmd(f"Pushing ref {ref} to {remote}", f"git push {remote} {ref}")


def merge_ref(ref, **kwargs): 
    __defaults(kwargs)
    cmd(f"Merging ref {ref}", f"git merge {ref}")
