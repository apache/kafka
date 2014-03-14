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
import thread

import system_test_utils

class TestcaseEnv():

    # ================================
    # Generic testcase environment
    # ================================

    # dictionary of entity_id to ppid for Zookeeper entities
    # key: entity_id
    # val: ppid of Zookeeper associated to that entity_id
    # { 0: 12345, 1: 12389, ... }
    entityZkParentPidDict = {}

    # dictionary of entity_id to ppid for broker entities
    # key: entity_id
    # val: ppid of broker associated to that entity_id
    # { 0: 12345, 1: 12389, ... }
    entityBrokerParentPidDict = {}

    # dictionary of entity_id to ppid for mirror-maker entities
    # key: entity_id
    # val: ppid of broker associated to that entity_id
    # { 0: 12345, 1: 12389, ... }
    entityMirrorMakerParentPidDict = {}

    # dictionary of entity_id to ppid for console-consumer entities
    # key: entity_id
    # val: ppid of console consumer associated to that entity_id
    # { 0: 12345, 1: 12389, ... }
    entityConsoleConsumerParentPidDict = {}

    # dictionary of entity_id to ppid for migration tool entities
    # key: entity_id
    # val: ppid of broker associated to that entity_id
    # { 0: 12345, 1: 12389, ... }
    entityMigrationToolParentPidDict = {}

    # dictionary of entity_id to list of JMX ppid
    # key: entity_id
    # val: list of JMX ppid associated to that entity_id
    # { 1: [1234, 1235, 1236], 2: [2234, 2235, 2236], ... }
    entityJmxParentPidDict = {}

    # dictionary of hostname-topic-ppid for consumer
    # key: hostname
    # val: dict of topic-ppid
    # { host1: { test1 : 12345 }, host1: { test2 : 12389 }, ... }
    consumerHostParentPidDict = {}

    # dictionary of hostname-topic-ppid for producer
    # key: hostname
    # val: dict of topic-ppid
    # { host1: { test1 : 12345 }, host1: { test2 : 12389 }, ... }
    producerHostParentPidDict = {}

    # list of testcase configs
    testcaseConfigsList = []

    # dictionary to keep track of testcase arguments such as replica_factor, num_partition
    testcaseArgumentsDict = {}


    def __init__(self, systemTestEnv, classInstance):
        self.systemTestEnv    = systemTestEnv

        # gather the test case related info and add to an SystemTestEnv object
        self.testcaseResultsDict = {}
        self.testcaseResultsDict["_test_class_name"] = classInstance.__class__.__name__
        self.testcaseResultsDict["_test_case_name"]  = ""
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
        self.testcasePropJsonPathName = ""
        self.testcaseNonEntityDataDict = {}

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

        # Lock object for producer threads synchronization
        self.lock = thread.allocate_lock()

        self.numProducerThreadsRunning = 0

        # to be used when validating data match - these variables will be
        # updated by kafka_system_test_utils.start_producer_in_thread
        self.producerTopicsString = ""
        self.consumerTopicsString = ""

    def initWithKnownTestCasePathName(self, testCasePathName):
        testcaseDirName = os.path.basename(testCasePathName)
        self.testcaseResultsDict["_test_case_name"] = testcaseDirName
        self.testCaseBaseDir = testCasePathName
        self.testCaseLogsDir = self.testCaseBaseDir + "/logs"
        self.testCaseDashboardsDir = self.testCaseBaseDir + "/dashboards"

        # find testcase properties json file
        self.testcasePropJsonPathName = system_test_utils.get_testcase_prop_json_pathname(testCasePathName)

        # get the dictionary that contains the testcase arguments and description
        self.testcaseNonEntityDataDict = system_test_utils.get_json_dict_data(self.testcasePropJsonPathName)

    def printTestCaseDescription(self, testcaseDirName):
        testcaseDescription = ""
        for k,v in self.testcaseNonEntityDataDict.items():
            if ( k == "description" ):
                testcaseDescription = v

        print "\n"
        print "======================================================================================="
        print "Test Case Name :", testcaseDirName
        print "======================================================================================="
        print "Description    :"
        for step in sorted(testcaseDescription.iterkeys()):
            print "   ", step, ":", testcaseDescription[step]
        print "======================================================================================="
        print "Test Case Args :"
        for k,v in self.testcaseArgumentsDict.items():
            print "   ", k, " : ", v
            self.testcaseResultsDict["arg : " + k] = v
        print "======================================================================================="


