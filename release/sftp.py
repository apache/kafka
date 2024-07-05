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
Auxiliary functions to interact with sftp(1).
"""

import subprocess

from runtime import (
    cmd,
    confirm_or_fail,
    execute,
    fail,
)

REMOTE_DIR = "public_html"


def mkdirp(apache_id, dir):
    cmd_desc = f"Creating '{dir}' in your Apache home directory"
    cmd_str = f"sftp -b - {apache_id}@home.apache.org"
    stdin_str  = f"mkdir {dir}\n"
    cmd(cmd_desc, cmd_str, stdin=stdin_str, allow_failure=True)


def upload(apache_id, destination, dir):
    cmd_desc = f"Uploading '{dir}' under {REMOTE_DIR} in your Apache home directory, this may take a while."
    cmd_str = f"sftp -b - {apache_id}@home.apache.org"
    stdin_str  = f"cd {destination}\nput -r {dir}\n"
    cmd(cmd_desc, cmd_str, stdin=stdin_str)


def upload_artifacts(apache_id, dir):
    mkdirp(apache_id, REMOTE_DIR)
    upload(apache_id, REMOTE_DIR, dir)
    confirm_or_fail(f"Are the artifacts present in your Apache home: https://home.apache.org/~{apache_id}/ ?")


def test(apache_id):
    """
    Test the ability to estalish an sftp session.
    """
    execute(f"sftp {apache_id}@home.apache.org", input="bye")

