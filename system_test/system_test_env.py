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
# system_test_env.py
# ===================================

import copy
import json
import os
import sys

from   utils import system_test_utils

class SystemTestEnv():

    # private:
    _cwdFullPath              = os.getcwd()
    _thisScriptFullPathName   = os.path.realpath(__file__)
    _thisScriptBaseDir        = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0])))

    # public:
    SYSTEM_TEST_BASE_DIR      = os.path.abspath(_thisScriptBaseDir)
    SYSTEM_TEST_UTIL_DIR      = os.path.abspath(SYSTEM_TEST_BASE_DIR + "/utils")
    SYSTEM_TEST_SUITE_SUFFIX  = "_testsuite"
    SYSTEM_TEST_CASE_PREFIX   = "testcase_"
    SYSTEM_TEST_MODULE_EXT    = ".py"
    CLUSTER_CONFIG_FILENAME   = "cluster_config.json"
    CLUSTER_CONFIG_PATHNAME   = os.path.abspath(SYSTEM_TEST_BASE_DIR + "/" + CLUSTER_CONFIG_FILENAME)
    METRICS_FILENAME          = "metrics.json"
    METRICS_PATHNAME          = os.path.abspath(SYSTEM_TEST_BASE_DIR + "/" + METRICS_FILENAME)
    TESTCASE_TO_RUN_FILENAME  = "testcase_to_run.json"
    TESTCASE_TO_RUN_PATHNAME  = os.path.abspath(SYSTEM_TEST_BASE_DIR + "/" + TESTCASE_TO_RUN_FILENAME)
    TESTCASE_TO_SKIP_FILENAME = "testcase_to_skip.json"
    TESTCASE_TO_SKIP_PATHNAME = os.path.abspath(SYSTEM_TEST_BASE_DIR + "/" + TESTCASE_TO_SKIP_FILENAME)

    clusterEntityConfigDictList                      = []   # cluster entity config for current level
    clusterEntityConfigDictListInSystemTestLevel     = []   # cluster entity config defined in system level
    clusterEntityConfigDictListLastFoundInTestSuite  = []   # cluster entity config last found in testsuite level
    clusterEntityConfigDictListLastFoundInTestCase   = []   # cluster entity config last found in testcase level

    systemTestResultsList        = []
    testCaseToRunListDict        = {}
    testCaseToSkipListDict       = {}

    printTestDescriptionsOnly    = False
    doNotValidateRemoteHost      = False

    def __init__(self):
        "Create an object with this system test session environment"

        # load the system level cluster config
        system_test_utils.load_cluster_config(self.CLUSTER_CONFIG_PATHNAME, self.clusterEntityConfigDictList)

        # save the system level cluster config
        self.clusterEntityConfigDictListInSystemTestLevel = copy.deepcopy(self.clusterEntityConfigDictList)

        # retrieve testcases to run from testcase_to_run.json
        try:
            testcaseToRunFileContent  = open(self.TESTCASE_TO_RUN_PATHNAME, "r").read()
            testcaseToRunData        = json.loads(testcaseToRunFileContent)
            for testClassName, caseList in testcaseToRunData.items():
                self.testCaseToRunListDict[testClassName] = caseList
        except:
            pass

        # retrieve testcases to skip from testcase_to_skip.json
        try:
            testcaseToSkipFileContent = open(self.TESTCASE_TO_SKIP_PATHNAME, "r").read()
            testcaseToSkipData        = json.loads(testcaseToSkipFileContent)
            for testClassName, caseList in testcaseToSkipData.items():
                self.testCaseToSkipListDict[testClassName] = caseList
        except:
            pass

    def isTestCaseToSkip(self, testClassName, testcaseDirName):
        testCaseToRunList  = {}
        testCaseToSkipList = {}

        try:
            testCaseToRunList  = self.testCaseToRunListDict[testClassName]
        except:
            # no 'testClassName' found => no need to run any cases for this test class
            return True

        try:
            testCaseToSkipList = self.testCaseToSkipListDict[testClassName]
        except:
            pass

        # if testCaseToRunList has elements, it takes precedence:
        if len(testCaseToRunList) > 0:
            #print "#### testClassName => ", testClassName
            #print "#### testCaseToRunList => ", testCaseToRunList
            #print "#### testcaseDirName => ", testcaseDirName
            if not testcaseDirName in testCaseToRunList:
                #self.log_message("Skipping : " + testcaseDirName)
                return True
        elif len(testCaseToSkipList) > 0:
            #print "#### testClassName => ", testClassName
            #print "#### testCaseToSkipList => ", testCaseToSkipList
            #print "#### testcaseDirName => ", testcaseDirName
            if testcaseDirName in testCaseToSkipList:
                #self.log_message("Skipping : " + testcaseDirName)
                return True

        return False


    def getSystemTestEnvDict(self):
        envDict = {}
        envDict["system_test_base_dir"]             = self.SYSTEM_TEST_BASE_DIR
        envDict["system_test_util_dir"]             = self.SYSTEM_TEST_UTIL_DIR
        envDict["cluster_config_pathname"]          = self.CLUSTER_CONFIG_PATHNAME
        envDict["system_test_suite_suffix"]         = self.SYSTEM_TEST_SUITE_SUFFIX
        envDict["system_test_case_prefix"]          = self.SYSTEM_TEST_CASE_PREFIX
        envDict["system_test_module_ext"]           = self.SYSTEM_TEST_MODULE_EXT
        envDict["cluster_config_pathname"]          = self.CLUSTER_CONFIG_PATHNAME
        envDict["cluster_entity_config_dict_list"]  = self.clusterEntityConfigDictList
        envDict["system_test_results_list"]         = self.systemTestResultsList
        return envDict


