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
# replica_basic_test.py
# ===================================

import inspect
import logging
import os
import signal
import subprocess
import sys
import time
import traceback

from   system_test_env    import SystemTestEnv
sys.path.append(SystemTestEnv.SYSTEM_TEST_UTIL_DIR)
from   setup_utils        import SetupUtils
import system_test_utils
from   testcase_env       import TestcaseEnv

# product specific: Kafka
import kafka_system_test_utils
import metrics

class ReplicaBasicTest(SetupUtils):

    testModuleAbsPathName = os.path.realpath(__file__)
    testSuiteAbsPathName  = os.path.abspath(os.path.dirname(testModuleAbsPathName))
    isLeaderLogPattern    = "Completed the leader state transition"
    brokerShutDownCompletedPattern = "shut down completed"

    def __init__(self, systemTestEnv):

        # SystemTestEnv - provides cluster level environment settings
        #     such as entity_id, hostname, kafka_home, java_home which
        #     are available in a list of dictionary named 
        #     "clusterEntityConfigDictList"
        self.systemTestEnv = systemTestEnv

        # dict to pass user-defined attributes to logger argument: "extra"
        d = {'name_of_class': self.__class__.__name__}

    def signal_handler(self, signal, frame):
        self.log_message("Interrupt detected - User pressed Ctrl+c")

        for entityId, parentPid in self.testcaseEnv.entityParentPidDict.items():
            kafka_system_test_utils.stop_remote_entity(self.systemTestEnv, self.testcaseEnv, entityId, parentPid)

        sys.exit(1) 

    def runTest(self):

        # get all testcase directories under this testsuite
        testCasePathNameList = system_test_utils.get_dir_paths_with_prefix(
            self.testSuiteAbsPathName, SystemTestEnv.SYSTEM_TEST_CASE_PREFIX)
        testCasePathNameList.sort()

        # =============================================================
        # launch each testcase one by one: testcase_1, testcase_2, ...
        # =============================================================
        for testCasePathName in testCasePathNameList:
   
            try: 
                # create a new instance of TestcaseEnv to keep track of this testcase's environment variables
                self.testcaseEnv = TestcaseEnv(self.systemTestEnv, self)
                self.testcaseEnv.testSuiteBaseDir = self.testSuiteAbsPathName
    
                # initialize self.testcaseEnv with user-defined environment
                self.testcaseEnv.userDefinedEnvVarDict["BROKER_SHUT_DOWN_COMPLETED_MSG"] = ReplicaBasicTest.brokerShutDownCompletedPattern
                self.testcaseEnv.userDefinedEnvVarDict["REGX_BROKER_SHUT_DOWN_COMPLETED_PATTERN"] = \
                    "\[(.*?)\] .* \[Kafka Server (.*?)\], " + ReplicaBasicTest.brokerShutDownCompletedPattern

                self.testcaseEnv.userDefinedEnvVarDict["LEADER_ELECTION_COMPLETED_MSG"] = ReplicaBasicTest.isLeaderLogPattern
                self.testcaseEnv.userDefinedEnvVarDict["REGX_LEADER_ELECTION_PATTERN"]  = \
                    "\[(.*?)\] .* Broker (.*?): " + \
                    self.testcaseEnv.userDefinedEnvVarDict["LEADER_ELECTION_COMPLETED_MSG"] + \
                    " for topic (.*?) partition (.*?) \(.*"

                self.testcaseEnv.userDefinedEnvVarDict["zkConnectStr"] = ""
    
                # find testcase properties json file
                testcasePropJsonPathName = system_test_utils.get_testcase_prop_json_pathname(testCasePathName)
                self.logger.debug("testcasePropJsonPathName : " + testcasePropJsonPathName, extra=self.d)
    
                # get the dictionary that contains the testcase arguments and description
                testcaseNonEntityDataDict = system_test_utils.get_json_dict_data(testcasePropJsonPathName)
    
                testcaseDirName = os.path.basename(testCasePathName)
                self.testcaseEnv.testcaseResultsDict["test_case_name"] = testcaseDirName
    
                # update testcaseEnv
                self.testcaseEnv.testCaseBaseDir = testCasePathName
                self.testcaseEnv.testCaseLogsDir = self.testcaseEnv.testCaseBaseDir + "/logs"
                self.testcaseEnv.testCaseDashboardsDir = self.testcaseEnv.testCaseBaseDir + "/dashboards"

                # get testcase description
                testcaseDescription = ""
                for k,v in testcaseNonEntityDataDict.items():
                    if ( k == "description" ): testcaseDescription = v
    
                # TestcaseEnv.testcaseArgumentsDict initialized, this dictionary keeps track of the
                # "testcase_args" in the testcase_properties.json such as replica_factor, num_partition, ...
                self.testcaseEnv.testcaseArgumentsDict = testcaseNonEntityDataDict["testcase_args"]
    
                # =================================================================
                # TestcaseEnv environment settings initialization are completed here
                # =================================================================
                # self.testcaseEnv.systemTestBaseDir
                # self.testcaseEnv.testSuiteBaseDir
                # self.testcaseEnv.testCaseBaseDir
                # self.testcaseEnv.testCaseLogsDir
                # self.testcaseEnv.testcaseArgumentsDict
    
                print
                # display testcase name and arguments
                self.log_message("Test Case : " + testcaseDirName)
                for k,v in self.testcaseEnv.testcaseArgumentsDict.items():
                    self.anonLogger.info("    " + k + " : " + v)
                self.log_message("Description : " + testcaseDescription)
   
                # ================================================================ #
                # ================================================================ #
                #            Product Specific Testing Code Starts Here:            #
                # ================================================================ #
                # ================================================================ #
    
                # initialize signal handler
                signal.signal(signal.SIGINT, self.signal_handler)
    
                # create "LOCAL" log directories for metrics, dashboards for each entity under this testcase
                # for collecting logs from remote machines
                kafka_system_test_utils.generate_testcase_log_dirs(self.systemTestEnv, self.testcaseEnv)
    
                # TestcaseEnv.testcaseConfigsList initialized by reading testcase properties file:
                #   system_test/<suite_name>_testsuite/testcase_<n>/testcase_<n>_properties.json
                self.testcaseEnv.testcaseConfigsList = system_test_utils.get_json_list_data(testcasePropJsonPathName)
    
                # TestcaseEnv - initialize producer & consumer config / log file pathnames
                kafka_system_test_utils.init_entity_props(self.systemTestEnv, self.testcaseEnv)
    
                # clean up data directories specified in zookeeper.properties and kafka_server_<n>.properties
                kafka_system_test_utils.cleanup_data_at_remote_hosts(self.systemTestEnv, self.testcaseEnv)

                # generate remote hosts log/config dirs if not exist
                kafka_system_test_utils.generate_testcase_log_dirs_in_remote_hosts(self.systemTestEnv, self.testcaseEnv)
    
                # generate properties files for zookeeper, kafka, producer, consumer:
                # 1. copy system_test/<suite_name>_testsuite/config/*.properties to 
                #    system_test/<suite_name>_testsuite/testcase_<n>/config/
                # 2. update all properties files in system_test/<suite_name>_testsuite/testcase_<n>/config
                #    by overriding the settings specified in:
                #    system_test/<suite_name>_testsuite/testcase_<n>/testcase_<n>_properties.json
                kafka_system_test_utils.generate_overriden_props_files(self.testSuiteAbsPathName, self.testcaseEnv, self.systemTestEnv)
    
                # =============================================
                # preparing all entities to start the test
                # =============================================
                self.log_message("starting zookeepers")
                kafka_system_test_utils.start_zookeepers(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 2s")
                time.sleep(2)
    
                self.log_message("starting brokers")
                kafka_system_test_utils.start_brokers(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 5s")
                time.sleep(5)
    
                self.log_message("creating topics")
                kafka_system_test_utils.create_topic(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 5s")
                time.sleep(5)
    
                self.log_message("looking up leader")
                leaderDict = kafka_system_test_utils.get_leader_elected_log_line(self.systemTestEnv, self.testcaseEnv)
    
                # ==========================
                # leaderDict looks like this:
                # ==========================
                #{'entity_id': u'3',
                # 'partition': '0',
                # 'timestamp': 1345050255.8280001,
                # 'hostname': u'localhost',
                # 'topic': 'test_1',
                # 'brokerid': '3'}
    
                # =============================================
                # validate to see if leader election is successful
                # =============================================
                self.log_message("validating leader election")
                result = kafka_system_test_utils.validate_leader_election_successful( \
                             self.testcaseEnv, leaderDict, self.testcaseEnv.validationStatusDict)
    
                # =============================================
                # get leader re-election latency
                # =============================================
                bounceLeaderFlag = self.testcaseEnv.testcaseArgumentsDict["bounce_leader"]
                self.log_message("bounce_leader flag : " + bounceLeaderFlag)
                if (bounceLeaderFlag.lower() == "true"):
                    reelectionLatency = kafka_system_test_utils.get_reelection_latency(self.systemTestEnv, self.testcaseEnv, leaderDict)

                # =============================================
                # starting producer 
                # =============================================
                self.log_message("starting producer in the background")
                kafka_system_test_utils.start_producer_performance(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 10s")
                time.sleep(10)
    
                # =============================================
                # starting previously terminated broker 
                # =============================================
                if bounceLeaderFlag.lower() == "true":
                    self.log_message("starting the previously terminated broker")
                    stoppedLeaderEntityId  = leaderDict["entity_id"]
                    kafka_system_test_utils.start_entity_in_background(self.systemTestEnv, self.testcaseEnv, stoppedLeaderEntityId)
                    self.anonLogger.info("sleeping for 5s")
                    time.sleep(5)

                # =============================================
                # starting consumer
                # =============================================
                self.log_message("starting consumer in the background")
                kafka_system_test_utils.start_console_consumer(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 10s")
                time.sleep(10)
                
                # this testcase is completed - so stopping all entities
                self.log_message("stopping all entities")
                for entityId, parentPid in self.testcaseEnv.entityParentPidDict.items():
                    kafka_system_test_utils.stop_remote_entity(self.systemTestEnv, entityId, parentPid)
                    
                # validate the data matched
                # =============================================
                self.log_message("validating data matched")
                result = kafka_system_test_utils.validate_data_matched(self.systemTestEnv, self.testcaseEnv)
                
                # =============================================
                # collect logs from remote hosts
                # =============================================
                kafka_system_test_utils.collect_logs_from_remote_hosts(self.systemTestEnv, self.testcaseEnv)
    
                # ==========================
                # draw graphs
                # ==========================
                metrics.draw_all_graphs(self.systemTestEnv.METRICS_PATHNAME, 
                                        self.testcaseEnv, 
                                        self.systemTestEnv.clusterEntityConfigDictList)
                
                # build dashboard, one for each role
                metrics.build_all_dashboards(self.systemTestEnv.METRICS_PATHNAME,
                                             self.testcaseEnv.testCaseDashboardsDir,
                                             self.systemTestEnv.clusterEntityConfigDictList)
                
                # stop metrics processes
                for entity in self.systemTestEnv.clusterEntityConfigDictList:
                    metrics.stop_metrics_collection(entity['hostname'], entity['jmx_port'])
                                        
            except Exception as e:
                self.log_message("Exception while running test {0}".format(e))
                traceback.print_exc()
                traceback.print_exc()

            finally:
                self.log_message("stopping all entities")

                for entityId, parentPid in self.testcaseEnv.entityParentPidDict.items():
                    kafka_system_test_utils.force_stop_remote_entity(self.systemTestEnv, entityId, parentPid)

                for entityId, jmxParentPidList in self.testcaseEnv.entityJmxParentPidDict.items():
                    for jmxParentPid in jmxParentPidList:
                        kafka_system_test_utils.force_stop_remote_entity(self.systemTestEnv, entityId, jmxParentPid)

                for hostname, consumerPPid in self.testcaseEnv.consumerHostParentPidDict.items():
                    consumerEntityId = system_test_utils.get_data_by_lookup_keyval( \
                        self.systemTestEnv.clusterEntityConfigDictList, "hostname", hostname, "entity_id")
                    kafka_system_test_utils.force_stop_remote_entity(self.systemTestEnv, consumerEntityId, consumerPPid)

                for hostname, producerPPid in self.testcaseEnv.producerHostParentPidDict.items():
                    producerEntityId = system_test_utils.get_data_by_lookup_keyval( \
                        self.systemTestEnv.clusterEntityConfigDictList, "hostname", hostname, "entity_id")
                    kafka_system_test_utils.force_stop_remote_entity(self.systemTestEnv, producerEntityId, producerPPid)

                #kafka_system_test_utils.ps_grep_terminate_running_entity(self.systemTestEnv)

