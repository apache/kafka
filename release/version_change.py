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

from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
import argparse
import re

parser = argparse.ArgumentParser(description='Update release.')

parser.add_argument('--version', help='version of the release')

args = parser.parse_args()

version = args.version


def update_template_js(file_name, version) -> None:
    parser = Parser()

    # read the file
    with open(file_name, "r") as file:
        data = file.read()
    with open(file_name, "r") as file:
        data_list = file.readlines()
    s = []
    for l in data_list:
        if l.startswith("var context"):
            break
        s.append(l)

    # parse the text
    tree = parser.parse(data)

    final_version = '"%s"' % version
    # node
    for prop in nodevisitor.visit(tree):
        for node in prop:
            if isinstance(node, ast.Assign):
                if (node.left.value == '"fullDotVersion"') and node.right.value != final_version:
                    node.right.value = final_version
                    break
    
    with open(file_name, "w") as file:
        file.write(tree.to_ecma())

    with open(file_name, "r") as file:
        data = file.read()
    s.extend(data)
    string = ''.join(s)
    with open(file_name, "w") as file:
        file.write(string)
    print("Updated %s!" % file_name)

def update_text(file_name, final_version, pattern, log_file_name = True) -> None:
    with open(file_name, "r") as file:
        file_prop = file.readlines()
    for index, line in enumerate(file_prop):
        if line.startswith(pattern):
            file_prop[index] = final_version
    string = ''.join(file_prop)
    with open(file_name, "w") as file:
        file.write(string)
    if log_file_name:
        print("Updated %s!" % file_name)

def update_pom_xml(file_name, final_version, pattern):
    with open(file_name, "r") as file:
        file_prop = file.readlines()
    index = 0
    element_pattern = pattern["element_pattern"]
    previous_element_pattern = pattern.get("previous_element_pattern", None)
    while index < len(file_prop) - 1:
        element = file_prop[index].strip()
        prev_element = file_prop[index-1].strip()
        if element.startswith(element_pattern):
            if (previous_element_pattern and prev_element.startswith(previous_element_pattern)) or \
                  not previous_element_pattern:
                file_prop[index] = final_version
                break
        index += 1
    string = ''.join(file_prop)
    with open(file_name, "w") as file:
        file.write(string)
    print("Updated %s!" % file_name)

def update_version_py(file_name, version):
    version_str = '"%s"' % version
    update_text(file_name, 'DEV_VERSION = KafkaVersion("%s-SNAPSHOT")\n' % version, "DEV_VERSION =", False)
    with open(file_name, "r") as file:
        file_data = file.readlines()

    # search for the latest version running
    last_line = file_data[-1].split(" ")
    latest_version = last_line[-1].strip()
    second_last_line = file_data[-2].split(" ")
    new_version_split = "V_%s" % version.replace(".", "_")

    # if the version is same, do nothing
    if latest_version == new_version_split:
        print("Same version was provided, no change made to version.py")
        return
    
    # if major version is same, make under the same header 3.x else create a new header
    if latest_version[:-2] == new_version_split[:-2]:
        second_last_line[0] = new_version_split
        if second_last_line[2].startswith("KafkaVersion"):
            second_last_line[2] = 'KafkaVersion(%s)\n' % version_str
        second_last_line_string = ' '.join(second_last_line)
        last_line = file_data[-1].split(" ")
        last_line[-1] = new_version_split
        last_line_string = ' '.join(last_line)
        del file_data[-1]
        file_data.append(second_last_line_string)
        file_data.append('%s\n'%last_line_string)
    else:
        file_data.append("\n")
        file_data.append("# %s.x versions\n" % version[:3])
        file_data.append("%s = KafkaVersion(%s)\n" % (new_version_split,version_str))
        file_data.append("LATEST_%s = %s" % (version.replace(".", "_")[:-2], new_version_split))
    file_data_string = ''.join(file_data)
    with open(file_name, "w") as file:
        file.write(file_data_string)
    print("Updated %s!" % file_name)


update_template_js("../docs/js/templateData.js", version)
update_text("../gradle.properties", "version=%s-SNAPSHOT\n" % version, "version=")
update_text("../kafka-merge-pr.py", 'DEFAULT_FIX_VERSION = os.environ.get("DEFAULT_FIX_VERSION", "%s")\n'% version, "DEFAULT_FIX_VERSION = ")
update_text("../tests/kafkatest/__init__.py", "__version__ = '%s.dev0'\n" % version, "__version__ = ")
update_pom_xml("../streams/quickstart/pom.xml",
                "    <version>%s-SNAPSHOT</version>\n" % version,
                  {"element_pattern": "<version>", "previous_element_pattern": "<packaging>pom</packaging>"})
update_pom_xml("../streams/quickstart/java/src/main/resources/archetype-resources/pom.xml",
                "        <kafka.version>%s-SNAPSHOT</kafka.version>\n" % version,
                  {"element_pattern": "<kafka.version>"})
update_pom_xml("../streams/quickstart/java/pom.xml",
                "    <version>%s-SNAPSHOT</version>\n" % version,
                  {"element_pattern": "<version>", "previous_element_pattern": "<artifactId>streams-quickstart</artifactId>"})
update_version_py("../tests/kafkatest/version.py", version)
