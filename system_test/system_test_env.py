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

import json
import os
import sys

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

    clusterEntityConfigDictList  = []
    systemTestResultsList        = []

    def __init__(self):
        "Create an object with this system test session environment"

        # retrieve each entity's data from cluster config json file
        # as "dict" and enter them into a "list"
        jsonFileContent = open(self.CLUSTER_CONFIG_PATHNAME, "r").read()
        jsonData        = json.loads(jsonFileContent)
        for key, cfgList in jsonData.items():
            if key == "cluster_config":
                for cfg in cfgList:
                    self.clusterEntityConfigDictList.append(cfg)


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


