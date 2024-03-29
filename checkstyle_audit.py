#!/usr/bin/env python3

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

import xml.etree.ElementTree as ET
from rstr import Rstr
from random import SystemRandom
import glob

"""
Utility for auditing the contents of checkstyle/suppressions.xml to find suppressions which are no longer necessary.

1. Install rstr (and other dependencies if you don't have them)

2. Edit checkstyle.xml to remove the following node and disable all suppressions
<module name="SuppressionFilter">
<property name="file" value="${config_loc}/suppressions.xml"/>
</module>

3. Run the following command to run checkstyle on all modules without any suppressions (should fail)
./gradlew checkstyleMain checkstyleTest --continue

4. Run the script as many times as necessary, iterating and editing suppressions.xml as you work through the warnings
./checkstyle_audit.py

5. Revert the change to checkstyle.xml as it's only for the script to function correctly
git checkout HEAD checkstyle/checkstyle.xml

6. Run checkstyle on the repo to verify the suppressions changes
./gradlew checkstyleMain checkstyleTest
"""


# For catching typos, enforcing consistency of rules
_known_checks = {
    "ImportControl",
    "AvoidStarImport",
    "ClassDataAbstractionCoupling",
    "ClassFanOutComplexity",
    "FinalLocalVariable",
    "ParameterNumber",
    "NPathComplexity",
    "BooleanExpressionComplexity",
    "CyclomaticComplexity",
    "dontUseSystemExit",
    "JavaNCSS",
    "StaticVariableName",
    "MemberName",
    "ParameterName",
    "LocalVariableName",
    "MethodLength",
    "WhitespaceAfter",
    "WhitespaceAround",
    "UnnecessaryParentheses",
    "DefaultComesLast"
}


def _re_to_set(rs, regex):
    subs = regex.replace(".java", "[.]java")
    out = set()
    for _ in range(200):
        s = rs.xeger(subs)
        if len(s) != 0:
            out.add(s)
    expected = regex.count("|") + 1
    if len(out) != expected:
        return set()
    return out


def main():
    suppressions = ET.parse("checkstyle/suppressions.xml")
    rs = Rstr(SystemRandom())
    expected = dict()
    too_general = set()
    total_errors = 0
    for suppression in suppressions.getroot():
        if "checks" in suppression.attrib:
            checks = _re_to_set(rs, suppression.attrib["checks"])
        elif "id" in suppression.attrib:
            checks = _re_to_set(rs, suppression.attrib["id"])
        else:
            continue
        if "files" not in suppression.attrib:
            continue
        files = _re_to_set(rs, suppression.attrib["files"])
        for check in checks:
            if check not in _known_checks:
                print("Unknown check", check)
            for file in files:
                if "generated" in file:
                    continue
                pair = (check, file)
                if pair in expected:
                    first = expected.get(pair)
                    print(pair, "covered by two overlapping rules")
                    print(first.attrib)
                    print(suppression.attrib)
                expected[pair] = suppression

    total = len(expected)

    reports = glob.glob("**/build/reports/checkstyle/*.xml", recursive=True)
    for report in reports:
        failures = ET.parse(report)
        for file in failures.getroot():
            path = file.attrib["name"]
            if "generated" in path:
                continue
            filename = path.split("/")[-1]
            for error in file:
                className = error.attrib["source"]
                check = className.split(".")[-1].replace("Check", "")
                pair = (check, filename)
                if pair[0] == "FinalLocalVariable" and "org/apache/kafka/streams" not in path:
                    continue
                if pair[0] == "ClassDataAbstractionCoupling" and "streams" in path and "test/" in path:
                    continue
                total_errors = total_errors + 1
                if pair in expected:
                    expected[pair] = None
                else:
                    too_general.add(pair)

    print(len(too_general), "/", total_errors, "errors missing suppressions")
    for pair in too_general:
        covered = [(check, shorter_file) for check, shorter_file in expected if check == pair[0] and shorter_file in pair[1]]
        if len(covered) == 0:
            print(pair, "is missing suppression (?)")
        else:
            print(pair, "is covered by other suppression", covered)
    unused = {k: expected[k] for k in expected if expected[k] is not None}
    print(len(unused), "/", total, "suppressions missing errors")
    for pair in unused:
            print("unused", pair)


if __name__ == "__main__":
    main()
