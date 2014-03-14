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
# mirror_maker_test.py
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
from   replication_utils  import ReplicationUtils
import system_test_utils
from   testcase_env       import TestcaseEnv

# product specific: Kafka
import kafka_system_test_utils
import metrics

class MirrorMakerTest(ReplicationUtils, SetupUtils):

    testModuleAbsPathName = os.path.realpath(__file__)
    testSuiteAbsPathName  = os.path.abspath(os.path.dirname(testModuleAbsPathName))

    def __init__(self, systemTestEnv):

        # SystemTestEnv - provides cluster level environment settings
        #     such as entity_id, hostname, kafka_home, java_home which
        #     are available in a list of dictionary named 
        #     "clusterEntityConfigDictList"
        self.systemTestEnv = systemTestEnv

        super(MirrorMakerTest, self).__init__(self)

        # dict to pass user-defined attributes to logger argument: "extra"
        d = {'name_of_class': self.__class__.__name__}

    def signal_handler(self, signal, frame):
        self.log_message("Interrupt detected - User pressed Ctrl+c")

        # perform the necessary cleanup here when user presses Ctrl+c and it may be product specific
        self.log_message("stopping all entities - please wait ...")
        kafka_system_test_utils.stop_all_remote_running_processes(self.systemTestEnv, self.testcaseEnv)
        sys.exit(1) 

    def runTest(self):

        # ======================================================================
        # get all testcase directories under this testsuite
        # ======================================================================
        testCasePathNameList = system_test_utils.get_dir_paths_with_prefix(
            self.testSuiteAbsPathName, SystemTestEnv.SYSTEM_TEST_CASE_PREFIX)
        testCasePathNameList.sort()

        replicationUtils = ReplicationUtils(self)

        # =============================================================
        # launch each testcase one by one: testcase_1, testcase_2, ...
        # =============================================================
        for testCasePathName in testCasePathNameList:
   
            skipThisTestCase = False

            try: 
                # ======================================================================
                # A new instance of TestcaseEnv to keep track of this testcase's env vars
                # and initialize some env vars as testCasePathName is available now
                # ======================================================================
                self.testcaseEnv = TestcaseEnv(self.systemTestEnv, self)
                self.testcaseEnv.testSuiteBaseDir = self.testSuiteAbsPathName
                self.testcaseEnv.initWithKnownTestCasePathName(testCasePathName)
                self.testcaseEnv.testcaseArgumentsDict = self.testcaseEnv.testcaseNonEntityDataDict["testcase_args"]

                # ======================================================================
                # SKIP if this case is IN testcase_to_skip.json or NOT IN testcase_to_run.json
                # ======================================================================
                testcaseDirName = self.testcaseEnv.testcaseResultsDict["_test_case_name"]

                if self.systemTestEnv.printTestDescriptionsOnly:
                    self.testcaseEnv.printTestCaseDescription(testcaseDirName)
                    continue
                elif self.systemTestEnv.isTestCaseToSkip(self.__class__.__name__, testcaseDirName):
                    self.log_message("Skipping : " + testcaseDirName)
                    skipThisTestCase = True
                    continue
                else:
                    self.testcaseEnv.printTestCaseDescription(testcaseDirName)
                    system_test_utils.setup_remote_hosts_with_testcase_level_cluster_config(self.systemTestEnv, testCasePathName)

                # ============================================================================== #
                # ============================================================================== #
                #                   Product Specific Testing Code Starts Here:                   #
                # ============================================================================== #
                # ============================================================================== #
    
                # initialize self.testcaseEnv with user-defined environment variables (product specific)
                self.testcaseEnv.userDefinedEnvVarDict["zkConnectStr"] = ""
                self.testcaseEnv.userDefinedEnvVarDict["stopBackgroundProducer"]    = False
                self.testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"] = False

                # initialize signal handler
                signal.signal(signal.SIGINT, self.signal_handler)

                # TestcaseEnv.testcaseConfigsList initialized by reading testcase properties file:
                #   system_test/<suite_name>_testsuite/testcase_<n>/testcase_<n>_properties.json
                self.testcaseEnv.testcaseConfigsList = system_test_utils.get_json_list_data(
                    self.testcaseEnv.testcasePropJsonPathName)
                 
                # clean up data directories specified in zookeeper.properties and kafka_server_<n>.properties
                kafka_system_test_utils.cleanup_data_at_remote_hosts(self.systemTestEnv, self.testcaseEnv)

                # create "LOCAL" log directories for metrics, dashboards for each entity under this testcase
                # for collecting logs from remote machines
                kafka_system_test_utils.generate_testcase_log_dirs(self.systemTestEnv, self.testcaseEnv)

                # TestcaseEnv - initialize producer & consumer config / log file pathnames
                kafka_system_test_utils.init_entity_props(self.systemTestEnv, self.testcaseEnv)

                # generate remote hosts log/config dirs if not exist
                kafka_system_test_utils.generate_testcase_log_dirs_in_remote_hosts(self.systemTestEnv, self.testcaseEnv)
    
                # generate properties files for zookeeper, kafka, producer, consumer and mirror-maker:
                # 1. copy system_test/<suite_name>_testsuite/config/*.properties to 
                #    system_test/<suite_name>_testsuite/testcase_<n>/config/
                # 2. update all properties files in system_test/<suite_name>_testsuite/testcase_<n>/config
                #    by overriding the settings specified in:
                #    system_test/<suite_name>_testsuite/testcase_<n>/testcase_<n>_properties.json
                kafka_system_test_utils.generate_overriden_props_files(self.testSuiteAbsPathName,
                    self.testcaseEnv, self.systemTestEnv)
               
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
                kafka_system_test_utils.create_topic_for_producer_performance(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 5s")
                time.sleep(5)

                
                self.log_message("starting mirror makers")
                kafka_system_test_utils.start_mirror_makers(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 10s")
                time.sleep(10)

                
                # =============================================
                # starting producer 
                # =============================================
                self.log_message("starting producer in the background")
                kafka_system_test_utils.start_producer_performance(self.systemTestEnv, self.testcaseEnv, False)
                msgProducingFreeTimeSec = self.testcaseEnv.testcaseArgumentsDict["message_producing_free_time_sec"]
                self.anonLogger.info("sleeping for " + msgProducingFreeTimeSec + " sec to produce some messages")
                time.sleep(int(msgProducingFreeTimeSec))

                # =============================================
                # A while-loop to bounce mirror maker as specified
                # by "num_iterations" in testcase_n_properties.json
                # =============================================
                i = 1
                numIterations = int(self.testcaseEnv.testcaseArgumentsDict["num_iteration"])
                bouncedEntityDownTimeSec = 15
                try:
                    bouncedEntityDownTimeSec = int(self.testcaseEnv.testcaseArgumentsDict["bounced_entity_downtime_sec"])
                except:
                    pass

                while i <= numIterations:

                    self.log_message("Iteration " + str(i) + " of " + str(numIterations))

                    # =============================================
                    # Bounce Mirror Maker if specified in testcase config
                    # =============================================
                    bounceMirrorMaker = self.testcaseEnv.testcaseArgumentsDict["bounce_mirror_maker"]
                    self.log_message("bounce_mirror_maker flag : " + bounceMirrorMaker)
                    if (bounceMirrorMaker.lower() == "true"):

                        clusterConfigList          = self.systemTestEnv.clusterEntityConfigDictList
                        mirrorMakerEntityIdList    = system_test_utils.get_data_from_list_of_dicts(
                                                     clusterConfigList, "role", "mirror_maker", "entity_id")
                        stoppedMirrorMakerEntityId = mirrorMakerEntityIdList[0]

                        mirrorMakerPPid = self.testcaseEnv.entityMirrorMakerParentPidDict[stoppedMirrorMakerEntityId]
                        self.log_message("stopping mirror maker : " + mirrorMakerPPid)
                        kafka_system_test_utils.stop_remote_entity(self.systemTestEnv, stoppedMirrorMakerEntityId, mirrorMakerPPid)
                        self.anonLogger.info("sleeping for " + str(bouncedEntityDownTimeSec) + " sec")
                        time.sleep(bouncedEntityDownTimeSec)

                        # starting previously terminated broker 
                        self.log_message("starting the previously terminated mirror maker")
                        kafka_system_test_utils.start_mirror_makers(self.systemTestEnv, self.testcaseEnv, stoppedMirrorMakerEntityId)

                    self.anonLogger.info("sleeping for 15s")
                    time.sleep(15)
                    i += 1
                # while loop

                # =============================================
                # tell producer to stop
                # =============================================
                self.testcaseEnv.lock.acquire()
                self.testcaseEnv.userDefinedEnvVarDict["stopBackgroundProducer"] = True
                time.sleep(1)
                self.testcaseEnv.lock.release()
                time.sleep(1)

                # =============================================
                # wait for producer thread's update of
                # "backgroundProducerStopped" to be "True"
                # =============================================
                while 1:
                    self.testcaseEnv.lock.acquire()
                    self.logger.info("status of backgroundProducerStopped : [" + \
                        str(self.testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"]) + "]", extra=self.d)
                    if self.testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"]:
                        time.sleep(1)
                        self.testcaseEnv.lock.release()
                        self.logger.info("all producer threads completed", extra=self.d)
                        break
                    time.sleep(1)
                    self.testcaseEnv.lock.release()
                    time.sleep(2)

                self.anonLogger.info("sleeping for 15s")
                time.sleep(15)
                self.anonLogger.info("terminate Mirror Maker")
                cmdStr = "ps auxw | grep Mirror | grep -v grep | tr -s ' ' | cut -f2 -d ' ' | xargs kill -15"
                subproc = system_test_utils.sys_call_return_subproc(cmdStr)
                for line in subproc.stdout.readlines():
                    line = line.rstrip('\n')
                    self.anonLogger.info("#### ["+line+"]")
                self.anonLogger.info("sleeping for 15s")
                time.sleep(15)

                # =============================================
                # starting consumer
                # =============================================
                self.log_message("starting consumer in the background")
                kafka_system_test_utils.start_console_consumer(self.systemTestEnv, self.testcaseEnv)
                self.anonLogger.info("sleeping for 10s")
                time.sleep(10)
                    
                # =============================================
                # this testcase is completed - stop all entities
                # =============================================
                self.log_message("stopping all entities")
                for entityId, parentPid in self.testcaseEnv.entityBrokerParentPidDict.items():
                    kafka_system_test_utils.stop_remote_entity(self.systemTestEnv, entityId, parentPid)

                for entityId, parentPid in self.testcaseEnv.entityZkParentPidDict.items():
                    kafka_system_test_utils.stop_remote_entity(self.systemTestEnv, entityId, parentPid)

                # make sure all entities are stopped
                kafka_system_test_utils.ps_grep_terminate_running_entity(self.systemTestEnv)

                # =============================================
                # collect logs from remote hosts
                # =============================================
                kafka_system_test_utils.collect_logs_from_remote_hosts(self.systemTestEnv, self.testcaseEnv)

                # =============================================
                # validate the data matched and checksum
                # =============================================
                self.log_message("validating data matched")
                kafka_system_test_utils.validate_data_matched(self.systemTestEnv, self.testcaseEnv, replicationUtils)
                kafka_system_test_utils.validate_broker_log_segment_checksum(self.systemTestEnv, self.testcaseEnv, "source")
                kafka_system_test_utils.validate_broker_log_segment_checksum(self.systemTestEnv, self.testcaseEnv, "target")

                # =============================================
                # draw graphs
                # =============================================
                metrics.draw_all_graphs(self.systemTestEnv.METRICS_PATHNAME, 
                                        self.testcaseEnv, 
                                        self.systemTestEnv.clusterEntityConfigDictList)
                
                # build dashboard, one for each role
                metrics.build_all_dashboards(self.systemTestEnv.METRICS_PATHNAME,
                                             self.testcaseEnv.testCaseDashboardsDir,
                                             self.systemTestEnv.clusterEntityConfigDictList)

            except Exception as e:
                self.log_message("Exception while running test {0}".format(e))
                traceback.print_exc()

            finally:
                if not skipThisTestCase and not self.systemTestEnv.printTestDescriptionsOnly:
                    self.log_message("stopping all entities - please wait ...")
                    kafka_system_test_utils.stop_all_remote_running_processes(self.systemTestEnv, self.testcaseEnv)

