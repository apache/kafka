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

import os
import tempfile
from shutil import move, rmtree
from os import remove, close
from io import open

def file_exists(node, file):
    """Quick and dirty check for existence of remote file."""
    try:
        node.account.ssh("cat " + file, allow_fail=False)
        return True
    except:
        return False


def line_count(node, file):
    """Return the line count of file on node"""
    out = [line for line in node.account.ssh_capture("wc -l %s" % file)]
    if len(out) != 1:
        raise Exception("Expected single line of output from wc -l")

    return int(out[0].strip().split(" ")[0])

def replace_in_file(file_path, pattern, subst):
    fh, abs_path = tempfile.mkstemp()
    with open(abs_path, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(pattern, subst))
    close(fh)
    remove(file_path)
    move(abs_path, file_path)

def scp(source_node, source_path, target_node, target_path, pattern=None, subst=None):
    """ Copy file from source node to destination node. Optionally, replaces 'pattern' with 'subst' while
    copying a file. Uses a temporary file on local node, since there is no mechanism to copy directly
    between two remote nodes.
    :param source_node: source node to copy from
    :param source_path: path on the source node to copy from
    :param target_node: destination node
    :param target_path: path on the destination node to copy to
    """
    try:
        local_temp_dir = tempfile.mkdtemp(dir="/tmp")
    except OSError as e:
        raise Exception("Failed to create temporary local directory to scp %s to %s: %s" % (source_path, target_path, e.strerror))

    local_temp_file = os.path.join(local_temp_dir, "tmpfile")

    try:
        source_node.account.scp_from(source_path, local_temp_file)
        if pattern is not None:
            if subst is None:
                raise Exception("Substitute string must be specified if pattern is specified")
            replace_in_file(local_temp_file, pattern, subst)
        target_node.account.scp_to(local_temp_file, target_path)
    finally:
        rmtree(local_temp_dir, ignore_errors=True)