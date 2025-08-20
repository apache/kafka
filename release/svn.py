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
Auxiliary functions to interact with svn(1).
"""

import os
import shutil
import subprocess

from runtime import cmd

SVN_DEV_URL="https://dist.apache.org/repos/dist/dev/kafka"

def delete_old_rc_directory_if_needed(rc_tag, src, work_dir):
    svn_dev = os.path.join(work_dir, "svn_dev")
    cmd_desc = f"Check if {rc_tag} exists in the subversion repository."
    cmd_str = f"svn info --show-item revision {SVN_DEV_URL}/{rc_tag}"
    if not cmd(cmd_desc, cmd_str, cwd = svn_dev, allow_failure = True):
        print(f"Nothing under {SVN_DEV_URL}/{rc_tag}. Continuing.")
        return
    cmd_desc = f"Committing the deletion of {SVN_DEV_URL}/{rc_tag} from the svn repo."
    cmd_str = f"svn delete -m Remove_{rc_tag} {SVN_DEV_URL}/{rc_tag}"
    cmd(cmd_desc, cmd_str, cwd = svn_dev)

def commit_artifacts(rc_tag, src, work_dir):
    delete_old_rc_directory_if_needed(rc_tag, src, work_dir)
    svn_dev = os.path.join(work_dir, "svn_dev")
    dst = os.path.join(svn_dev, rc_tag)
    print(f"Copying {src} to {dst}")
    shutil.copytree(src, dst)
    cmd_desc = f"Adding {SVN_DEV_URL}/{rc_tag} to the svn repo."
    cmd_str = f"svn add ./{rc_tag}"
    cmd(cmd_desc, cmd_str, cwd = svn_dev)
    cmd_desc = f"Committing the addition of {SVN_DEV_URL}/{rc_tag} to the svn repo. Please wait, this may take a while."
    cmd_str = f"svn commit -m Add_{rc_tag}"
    cmd(cmd_desc, cmd_str, cwd = svn_dev)

def checkout_svn_dev(work_dir):
    svn_dev = os.path.join(work_dir, "svn_dev")
    if os.path.exists(svn_dev):
        shutil.rmtree(svn_dev)
    cmd_desc = f"Checking out {SVN_DEV_URL} at {svn_dev}"
    cmd_str = f"svn checkout --depth empty {SVN_DEV_URL}/ {svn_dev}"
    cmd(cmd_desc, cmd_str)
