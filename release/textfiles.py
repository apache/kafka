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
Auxiliary functions to access and manipulate text files.
"""

import re

from jproperties import Properties


def read(file_path):
    with open(file_path) as f:
        return f.read()


def write(file_path, content):
    with open(file_path, "w") as f:
        f.write(content)


def props(properties_text):
    """
    Load the keys and values into a dictionary from a .properties file.
    """
    props = Properties()
    props.load(properties_text, "utf-8")
    return props


def prop(filepath, propname):
    """
    Read the value for a given key in a .properties file.
    """
    values = props(read(filepath))
    value, _ = values[propname]
    return value


def replace(path, pattern, replacement, **kwargs):
    """
    Replace all occurrences of a text pattern in a text file.
    """
    is_regex = kwargs.get("regex", False)
    updated = []
    with open(path, "r") as f:
        for line in f:
            modified = line
            if is_regex:
                modified = re.sub(pattern, replacement, line)
            elif line.startswith(pattern):
                modified = replacement + "\n"

            updated.append(modified)

    with open(path, "w") as f:
        for line in updated:
            f.write(line)
