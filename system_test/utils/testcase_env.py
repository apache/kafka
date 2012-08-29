# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#!/usr/bin/env python

# ===================================
# testcase_env.py
# ===================================

import json
import os
import sys

class TestcaseEnv():

    # ================================
    # Generic testcase environment
    # ================================

    # dictionary of entity parent pid
    entityParentPidDict = {}

    # list of testcase configs
    testcaseConfigsList = []

    # dictionary to keep track of testcase arguments such as replica_factor, num_partition
    testcaseArgumentsDict = {}


    def __init__(self, systemTestEnv, classInstance):
        self.systemTestEnv    = systemTestEnv

        # gather the test case related info and add to an SystemTestEnv object
        self.testcaseResultsDict = {}
        self.testcaseResultsDict["test_class_name"]    = classInstance.__class__.__name__
        self.testcaseResultsDict["test_case_name"]     = ""
        self.validationStatusDict                      = {}
        self.testcaseResultsDict["validation_status"]  = self.validationStatusDict
        self.systemTestEnv.systemTestResultsList.append(self.testcaseResultsDict)

        # FIXME: in a distributed environement, kafkaBaseDir could be different in individual host
        #        => TBD
        self.kafkaBaseDir      = ""

        self.systemTestBaseDir = systemTestEnv.SYSTEM_TEST_BASE_DIR

        # to be initialized in the Test Module
        self.testSuiteBaseDir      = ""
        self.testCaseBaseDir       = ""
        self.testCaseLogsDir       = ""
        self.testCaseDashboardsDir = ""
        # ================================
        # dictionary to keep track of
        # user-defined environment variables
        # ================================

        # LEADER_ELECTION_COMPLETED_MSG = "completed the leader state transition"
        # REGX_LEADER_ELECTION_PATTERN  = "\[(.*?)\] .* Broker (.*?) " + \
        #                            LEADER_ELECTION_COMPLETED_MSG + \
        #                            " for topic (.*?) partition (.*?) \(.*"
        # zkConnectStr = ""
        # consumerLogPathName    = ""
        # consumerConfigPathName = ""
        # producerLogPathName    = ""
        # producerConfigPathName = ""

        self.userDefinedEnvVarDict = {}

