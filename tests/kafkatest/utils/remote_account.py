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
import shutil

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

def scp(source_node, source_path, target_node, target_path):
    """ Copy file from source node to destination node. Uses a temporary file
    on local node, since there is no mechanism to copy directly between two remote nodes.
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
        target_node.account.scp_to(local_temp_file, target_path)
    finally:
        shutil.rmtree(local_temp_dir, ignore_errors=True)
