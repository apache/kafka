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

import io
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from junit import parse_report


SAMPLE_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="TestSuite" tests="4" skipped="0" failures="0" errors="0" time="1.0">
    <testcase name="testZebra" classname="org.apache.kafka.ZebraTest" time="0.1"/>
    <testcase name="testBanana" classname="org.apache.kafka.AlphaTest" time="0.2"/>
    <testcase name="testApple" classname="org.apache.kafka.AlphaTest" time="0.3"/>
    <testcase name="testMango" classname="org.apache.kafka.MangoTest" time="0.4"/>
</testsuite>
"""


def build_quarantined_table(xml: str):
    """Simulate how the main block builds quarantined_table from passed tests."""
    fp = io.StringIO(xml)
    quarantined_table = []
    for suite in parse_report("/workspace", "build/junit-xml/module/flaky/TEST-foo.xml", fp):
        for test in suite.passed_tests:
            simple_class_name = test.class_name.split(".")[-1]
            quarantined_table.append((simple_class_name, test.test_name))
    return quarantined_table


def test_quarantined_table_unsorted_before_sort():
    """Verify the raw quarantined_table is not already sorted (test order follows XML order)."""
    table = build_quarantined_table(SAMPLE_XML)
    assert table != sorted(table), "Expected unsorted quarantined_table before sorting"


def test_quarantined_table_sorted_by_class_name_then_test_name():
    """KAFKA-18574: quarantined test list should be sorted by class_name and test_name."""
    table = build_quarantined_table(SAMPLE_XML)
    table.sort()
    assert table == [
        ("AlphaTest", "testApple"),
        ("AlphaTest", "testBanana"),
        ("MangoTest", "testMango"),
        ("ZebraTest", "testZebra"),
    ]
