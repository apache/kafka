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
# kafka_system_test_utils.py
# ===================================

import datetime
import getpass
import hashlib
import inspect
import json
import logging
import os
import pprint
import re
import subprocess
import sys
import thread
import time
import traceback

import system_test_utils
import metrics

from datetime  import datetime
from time      import mktime

# ====================================================================
# Two logging formats are defined in system_test/system_test_runner.py
# ====================================================================

# 1. "namedLogger" is defined to log message in this format:
#    "%(asctime)s - %(levelname)s - %(message)s %(name_of_class)s"
#    usage: to log message and showing the class name of the message

logger     = logging.getLogger("namedLogger")
thisClassName = '(kafka_system_test_utils)'
d = {'name_of_class': thisClassName}

# 2. "anonymousLogger" is defined to log message in this format:
#    "%(asctime)s - %(levelname)s - %(message)s"
#    usage: to log message without showing class name and it's appropriate
#           for logging generic message such as "sleeping for 5 seconds"

anonLogger = logging.getLogger("anonymousLogger")


# =====================================
# Sample usage of getting testcase env
# =====================================
def get_testcase_env(testcaseEnv):
    anonLogger.info("================================================")
    anonLogger.info("systemTestBaseDir     : " + testcaseEnv.systemTestBaseDir)
    anonLogger.info("testSuiteBaseDir      : " + testcaseEnv.testSuiteBaseDir)
    anonLogger.info("testCaseBaseDir       : " + testcaseEnv.testCaseBaseDir)
    anonLogger.info("testCaseLogsDir       : " + testcaseEnv.testCaseLogsDir)
    anonLogger.info("userDefinedEnvVarDict : (testcaseEnv.userDefinedEnvVarDict)")
    anonLogger.info("================================================")


def get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, type):

    defaultLogDir = testcaseEnv.testCaseLogsDir + "/" + role + "-" + entityId

    # type is either "metrics" or "dashboards" or "default"
    if type == "metrics":
        return testcaseEnv.testCaseLogsDir + "/" + role + "-" + entityId + "/metrics"
    elif type == "log_segments" :
        return testcaseEnv.testCaseLogsDir + "/" + role + "-" + entityId + "/log_segments"
    elif type == "default" :
        return testcaseEnv.testCaseLogsDir + "/" + role + "-" + entityId
    elif type == "dashboards":
        return testcaseEnv.testCaseLogsDir + "/dashboards"
    elif type == "config":
        return testcaseEnv.testCaseBaseDir + "/config"
    else:
        logger.error("unrecognized log directory type : " + type, extra=d)
        logger.error("returning default log dir : " + defaultLogDir, extra=d)
        return defaultLogDir


def generate_testcase_log_dirs(systemTestEnv, testcaseEnv):

    testcasePathName = testcaseEnv.testCaseBaseDir
    logger.debug("testcase pathname: " + testcasePathName, extra=d)

    if not os.path.exists(testcasePathName + "/config") : os.makedirs(testcasePathName + "/config")
    if not os.path.exists(testcasePathName + "/logs")   : os.makedirs(testcasePathName + "/logs")
    if not os.path.exists(testcasePathName + "/dashboards")   : os.makedirs(testcasePathName + "/dashboards")

    dashboardsPathName = testcasePathName + "/dashboards"
    if not os.path.exists(dashboardsPathName) : os.makedirs(dashboardsPathName)

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:
        entityId = clusterEntityConfigDict["entity_id"]
        role     = clusterEntityConfigDict["role"]

        metricsPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "metrics")
        if not os.path.exists(metricsPathName) : os.makedirs(metricsPathName)

        # create the role directory under dashboards
        dashboardsRoleDir = dashboardsPathName + "/" + role
        if not os.path.exists(dashboardsRoleDir) : os.makedirs(dashboardsRoleDir)


def collect_logs_from_remote_hosts(systemTestEnv, testcaseEnv):
    anonLogger.info("================================================")
    anonLogger.info("collecting logs from remote machines")
    anonLogger.info("================================================")

    testCaseBaseDir = testcaseEnv.testCaseBaseDir
    tcConfigsList   = testcaseEnv.testcaseConfigsList

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:
        hostname   = clusterEntityConfigDict["hostname"]
        entity_id  = clusterEntityConfigDict["entity_id"]
        role       = clusterEntityConfigDict["role"]
        kafkaHome  = clusterEntityConfigDict["kafka_home"]

        logger.debug("entity_id : " + entity_id, extra=d)
        logger.debug("hostname  : " + hostname,  extra=d)
        logger.debug("role      : " + role,      extra=d)

        configPathName     = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "config")
        metricsPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "metrics")
        logPathName        = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "default")
        rmtLogPathName     = logPathName
        rmtMetricsPathName = metricsPathName

        if hostname != "localhost":
            rmtConfigPathName  = replace_kafka_home(configPathName, kafkaHome)
            rmtMetricsPathName = replace_kafka_home(metricsPathName, kafkaHome)
            rmtLogPathName     = replace_kafka_home(logPathName, kafkaHome)

        # ==============================
        # collect entity log file
        # ==============================
        cmdList = ["scp",
                   hostname + ":" + rmtLogPathName + "/*",
                   logPathName]
        cmdStr  = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)

        # ==============================
        # collect entity metrics file
        # ==============================
        cmdList = ["scp",
                   hostname + ":" + rmtMetricsPathName + "/*",
                   metricsPathName]
        cmdStr  = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)

        # ==============================
        # collect broker log segment file
        # ==============================
        if role == "broker":
            dataLogPathName = system_test_utils.get_data_by_lookup_keyval(
                                  testcaseEnv.testcaseConfigsList, "entity_id", entity_id, "log.dir")

            cmdList = ["scp -r",
                       hostname + ":" + dataLogPathName,
                       logPathName]
            cmdStr  = " ".join(cmdList)
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

        # ==============================
        # collect ZK log
        # ==============================
        if role == "zookeeper":
            dataLogPathName = system_test_utils.get_data_by_lookup_keyval(
                                  testcaseEnv.testcaseConfigsList, "entity_id", entity_id, "dataDir")

            cmdList = ["scp -r",
                       hostname + ":" + dataLogPathName,
                       logPathName]
            cmdStr  = " ".join(cmdList)
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

    # ==============================
    # collect dashboards file
    # ==============================
    dashboardsPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "dashboards")
    rmtDashboardsPathName = dashboardsPathName

    if hostname != "localhost":
        rmtDashboardsPathName  = replace_kafka_home(dashboardsPathName, kafkaHome)

    cmdList = ["scp",
               hostname + ":" + rmtDashboardsPathName + "/*",
               dashboardsPathName]
    cmdStr  = " ".join(cmdList)
    logger.debug("executing command [" + cmdStr + "]", extra=d)
    system_test_utils.sys_call(cmdStr)


def generate_testcase_log_dirs_in_remote_hosts(systemTestEnv, testcaseEnv):
    testCaseBaseDir = testcaseEnv.testCaseBaseDir

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:
        hostname   = clusterEntityConfigDict["hostname"]
        entity_id  = clusterEntityConfigDict["entity_id"]
        role       = clusterEntityConfigDict["role"]
        kafkaHome  = clusterEntityConfigDict["kafka_home"]

        logger.debug("entity_id : " + entity_id, extra=d)
        logger.debug("hostname  : " + hostname, extra=d)
        logger.debug("role      : " + role, extra=d)

        configPathName     = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "config")
        metricsPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "metrics")
        dashboardsPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "dashboards")

        if hostname != "localhost":
            configPathName     = replace_kafka_home(configPathName, kafkaHome)
            metricsPathName    = replace_kafka_home(metricsPathName, kafkaHome)
            dashboardsPathName = replace_kafka_home(dashboardsPathName, kafkaHome)

        cmdList = ["ssh " + hostname,
                   "'mkdir -p",
                   configPathName,
                   metricsPathName,
                   dashboardsPathName + "'"]
        cmdStr  = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)


def init_entity_props(systemTestEnv, testcaseEnv):
    clusterConfigsList  = systemTestEnv.clusterEntityConfigDictList
    testcaseConfigsList = testcaseEnv.testcaseConfigsList
    testcasePathName    = testcaseEnv.testCaseBaseDir

    try:
        # consumer config / log files location
        consEntityIdList   = system_test_utils.get_data_from_list_of_dicts( \
                             clusterConfigsList, "role", "console_consumer", "entity_id")
        consLogList        = system_test_utils.get_data_from_list_of_dicts( \
                             testcaseConfigsList, "entity_id", consEntityIdList[0], "log_filename")
        consLogPathname    = testcasePathName + "/logs/" + consLogList[0]
        consCfgList        = system_test_utils.get_data_from_list_of_dicts( \
                             testcaseConfigsList, "entity_id", consEntityIdList[0], "config_filename")
        consCfgPathname    = testcasePathName + "/config/" + consCfgList[0]

        # producer config / log files location
        prodEntityIdList   = system_test_utils.get_data_from_list_of_dicts( \
                             clusterConfigsList, "role", "producer_performance", "entity_id")
        prodLogList        = system_test_utils.get_data_from_list_of_dicts( \
                             testcaseConfigsList, "entity_id", prodEntityIdList[0], "log_filename")
        prodLogPathname    = testcasePathName + "/logs/" + prodLogList[0]
        prodCfgList        = system_test_utils.get_data_from_list_of_dicts( \
                             testcaseConfigsList, "entity_id", prodEntityIdList[0], "config_filename")
        prodCfgPathname    = testcasePathName + "/config/" + prodCfgList[0]
    except:
        logger.error("Failed to initialize entity config/log path names: possibly mismatched " \
                    + "number of entities in cluster_config.json & testcase_n_properties.json", extra=d)
        raise

    testcaseEnv.userDefinedEnvVarDict["consumerLogPathName"]    = consLogPathname
    testcaseEnv.userDefinedEnvVarDict["consumerConfigPathName"] = consCfgPathname
    testcaseEnv.userDefinedEnvVarDict["producerLogPathName"]    = prodLogPathname
    testcaseEnv.userDefinedEnvVarDict["producerConfigPathName"] = prodCfgPathname


def copy_file_with_dict_values(srcFile, destFile, dictObj, keyValToAddDict):
    infile  = open(srcFile, "r")
    inlines = infile.readlines()
    infile.close()

    outfile = open(destFile, 'w')
    for line in inlines:
        for key in dictObj.keys():
            if (line.startswith(key + "=")):
                line = key + "=" + dictObj[key] + "\n"
        outfile.write(line)

    if (keyValToAddDict is not None):
        for key in sorted(keyValToAddDict.iterkeys()):
            line = key + "=" + keyValToAddDict[key] + "\n"
            outfile.write(line)

    outfile.close()

def generate_overriden_props_files(testsuitePathname, testcaseEnv, systemTestEnv):
    logger.info("calling generate_properties_files", extra=d)

    clusterConfigsList = systemTestEnv.clusterEntityConfigDictList
    tcPathname    = testcaseEnv.testCaseBaseDir
    tcConfigsList = testcaseEnv.testcaseConfigsList

    cfgTemplatePathname = os.path.abspath(testsuitePathname + "/config")
    cfgDestPathname     = os.path.abspath(tcPathname + "/config")
    logger.info("config template (source) pathname : " + cfgTemplatePathname, extra=d)
    logger.info("testcase config (dest)   pathname : " + cfgDestPathname, extra=d)

    # loop through all zookeepers (if more than 1) to retrieve host and clientPort
    # to construct a zookeeper.connect str for broker in the form of:
    # zookeeper.connect=<host1>:<port1>,<host2>:<port2>,...
    testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]        = ""
    testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]        = ""
    testcaseEnv.userDefinedEnvVarDict["sourceZkEntityIdList"]      = []
    testcaseEnv.userDefinedEnvVarDict["targetZkEntityIdList"]      = []
    testcaseEnv.userDefinedEnvVarDict["sourceZkHostPortDict"]      = {}
    testcaseEnv.userDefinedEnvVarDict["targetZkHostPortDict"]      = {}
    testcaseEnv.userDefinedEnvVarDict["sourceBrokerEntityIdList"]  = []
    testcaseEnv.userDefinedEnvVarDict["targetBrokerEntityIdList"]  = []
    testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"]          = ""
    testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]          = ""

    # update zookeeper cluster info into "testcaseEnv.userDefinedEnvVarDict"
    zkDictList = system_test_utils.get_dict_from_list_of_dicts(clusterConfigsList, "role", "zookeeper")

    for zkDict in zkDictList:
        entityID       = zkDict["entity_id"]
        hostname       = zkDict["hostname"]
        clusterName    = zkDict["cluster_name"]
        clientPortList = system_test_utils.get_data_from_list_of_dicts(tcConfigsList, "entity_id", entityID, "clientPort")
        clientPort     = clientPortList[0]

        if clusterName == "source":
            # update source cluster zookeeper entities
            testcaseEnv.userDefinedEnvVarDict["sourceZkEntityIdList"].append(entityID)
            if ( len(testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]) == 0 ):
                testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"] = hostname + ":" + clientPort
            else:
                testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"] += "," + hostname + ":" + clientPort

            # generate these strings for zookeeper config:
            # server.1=host1:2180:2182
            # server.2=host2:2180:2182
            zkClusterSize = len(testcaseEnv.userDefinedEnvVarDict["sourceZkHostPortDict"])
            zkClusterId   = str(zkClusterSize + 1)
            key           = "server." + zkClusterId
            val           = hostname + ":" + str(int(clientPort) - 1) + ":" + str(int(clientPort) + 1)
            testcaseEnv.userDefinedEnvVarDict["sourceZkHostPortDict"][key] = val

        elif clusterName == "target":
            # update target cluster zookeeper entities
            testcaseEnv.userDefinedEnvVarDict["targetZkEntityIdList"].append(entityID)
            if ( len(testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]) == 0 ):
                testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"] = hostname + ":" + clientPort
            else:
                testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"] += "," + hostname + ":" + clientPort

            # generate these strings for zookeeper config:
            # server.1=host1:2180:2182
            # server.2=host2:2180:2182
            zkClusterSize = len(testcaseEnv.userDefinedEnvVarDict["targetZkHostPortDict"])
            zkClusterId   = str(zkClusterSize + 1)
            key           = "server." + zkClusterId
            val           = hostname + ":" + str(int(clientPort) - 1) + ":" + str(int(clientPort) + 1)
            testcaseEnv.userDefinedEnvVarDict["targetZkHostPortDict"][key] = val

        else:
            logger.error("Invalid cluster name: " + clusterName, extra=d)
            raise Exception("Invalid cluster name : " + clusterName)
            sys.exit(1)

    # update broker cluster info into "testcaseEnv.userDefinedEnvVarDict"
    brokerDictList = system_test_utils.get_dict_from_list_of_dicts(clusterConfigsList, "role", "broker")
    for brokerDict in brokerDictList:
        entityID       = brokerDict["entity_id"]
        hostname       = brokerDict["hostname"]
        clusterName    = brokerDict["cluster_name"]
        portList       = system_test_utils.get_data_from_list_of_dicts(tcConfigsList, "entity_id", entityID, "port")
        port           = portList[0]

        if clusterName == "source":
            if ( len(testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"]) == 0 ):
                testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"] = hostname + ":" + port
            else:
                testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"] += "," + hostname + ":" + port
        elif clusterName == "target":
            if ( len(testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]) == 0 ):
                testcaseEnv.userDefinedEnvVarDict["targetBrokerList"] = hostname + ":" + port
            else:
                testcaseEnv.userDefinedEnvVarDict["targetBrokerList"] += "," + hostname + ":" + port
        else:
            logger.error("Invalid cluster name: " + clusterName, extra=d)
            raise Exception("Invalid cluster name : " + clusterName)
            sys.exit(1)

    # for each entity in the cluster config
    for clusterCfg in clusterConfigsList:
        cl_entity_id = clusterCfg["entity_id"]

        # loop through testcase config list 'tcConfigsList' for a matching cluster entity_id
        for tcCfg in tcConfigsList:
            if (tcCfg["entity_id"] == cl_entity_id):

                # copy the associated .properties template, update values, write to testcase_<xxx>/config

                if (clusterCfg["role"] == "broker"):
                    brokerVersion = "0.8"
                    try:
                        brokerVersion = tcCfg["version"]
                    except:
                        pass

                    if (brokerVersion == "0.7"):
                        if clusterCfg["cluster_name"] == "source":
                            tcCfg["zk.connect"] = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
                        else:
                            logger.error("Unknown cluster name for 0.7: " + clusterName, extra=d)
                            sys.exit(1)
                    else:
                        if clusterCfg["cluster_name"] == "source":
                            tcCfg["zookeeper.connect"] = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
                        elif clusterCfg["cluster_name"] == "target":
                            tcCfg["zookeeper.connect"] = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
                        else:
                            logger.error("Unknown cluster name: " + clusterName, extra=d)
                            sys.exit(1)

                    addedCSVConfig = {}
                    addedCSVConfig["kafka.csv.metrics.dir"] = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", clusterCfg["entity_id"], "metrics")
                    addedCSVConfig["kafka.metrics.polling.interval.secs"] = "5"
                    addedCSVConfig["kafka.metrics.reporters"] = "kafka.metrics.KafkaCSVMetricsReporter"
                    addedCSVConfig["kafka.csv.metrics.reporter.enabled"] = "true"

                    if brokerVersion == "0.7":
                        addedCSVConfig["brokerid"] = tcCfg["brokerid"]

                    copy_file_with_dict_values(cfgTemplatePathname + "/server.properties",
                        cfgDestPathname + "/" + tcCfg["config_filename"], tcCfg, addedCSVConfig)

                elif ( clusterCfg["role"] == "zookeeper"):
                    if clusterCfg["cluster_name"] == "source":
                        copy_file_with_dict_values(cfgTemplatePathname + "/zookeeper.properties",
                            cfgDestPathname + "/" + tcCfg["config_filename"], tcCfg,
                            testcaseEnv.userDefinedEnvVarDict["sourceZkHostPortDict"])
                    elif clusterCfg["cluster_name"] == "target":
                        copy_file_with_dict_values(cfgTemplatePathname + "/zookeeper.properties",
                            cfgDestPathname + "/" + tcCfg["config_filename"], tcCfg,
                            testcaseEnv.userDefinedEnvVarDict["targetZkHostPortDict"])
                    else:
                        logger.error("Unknown cluster name: " + clusterName, extra=d)
                        sys.exit(1)

                elif ( clusterCfg["role"] == "mirror_maker"):
                    tcCfg["metadata.broker.list"] = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]
                    tcCfg["bootstrap.servers"] = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"] # for new producer
                    copy_file_with_dict_values(cfgTemplatePathname + "/mirror_producer.properties",
                        cfgDestPathname + "/" + tcCfg["mirror_producer_config_filename"], tcCfg, None)

                    # update zookeeper.connect with the zk entities specified in cluster_config.json
                    tcCfg["zookeeper.connect"] = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
                    copy_file_with_dict_values(cfgTemplatePathname + "/mirror_consumer.properties",
                        cfgDestPathname + "/" + tcCfg["mirror_consumer_config_filename"], tcCfg, None)

                else:
                    logger.debug("UNHANDLED role " + clusterCfg["role"], extra=d)

    # scp updated config files to remote hosts
    scp_file_to_remote_host(clusterConfigsList, testcaseEnv)


def scp_file_to_remote_host(clusterEntityConfigDictList, testcaseEnv):

    testcaseConfigsList = testcaseEnv.testcaseConfigsList

    for clusterEntityConfigDict in clusterEntityConfigDictList:
        hostname         = clusterEntityConfigDict["hostname"]
        kafkaHome        = clusterEntityConfigDict["kafka_home"]
        localTestcasePathName  = testcaseEnv.testCaseBaseDir
        remoteTestcasePathName = localTestcasePathName

        if hostname != "localhost":
            remoteTestcasePathName = replace_kafka_home(localTestcasePathName, kafkaHome)

        cmdStr = "scp " + localTestcasePathName + "/config/* " + hostname + ":" + remoteTestcasePathName + "/config"
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)


def start_zookeepers(systemTestEnv, testcaseEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    zkEntityIdList = system_test_utils.get_data_from_list_of_dicts(
        clusterEntityConfigDictList, "role", "zookeeper", "entity_id")

    for zkEntityId in zkEntityIdList:
        configPathName = get_testcase_config_log_dir_pathname(testcaseEnv, "zookeeper", zkEntityId, "config")
        configFile     = system_test_utils.get_data_by_lookup_keyval(
                             testcaseEnv.testcaseConfigsList, "entity_id", zkEntityId, "config_filename")
        clientPort     = system_test_utils.get_data_by_lookup_keyval(
                             testcaseEnv.testcaseConfigsList, "entity_id", zkEntityId, "clientPort")
        dataDir        = system_test_utils.get_data_by_lookup_keyval(
                             testcaseEnv.testcaseConfigsList, "entity_id", zkEntityId, "dataDir")
        hostname       = system_test_utils.get_data_by_lookup_keyval(
                             clusterEntityConfigDictList, "entity_id", zkEntityId, "hostname")
        minusOnePort   = str(int(clientPort) - 1)
        plusOnePort    = str(int(clientPort) + 1)

        # read configFile to find out the id of the zk and create the file "myid"
        infile  = open(configPathName + "/" + configFile, "r")
        inlines = infile.readlines()
        infile.close()

        for line in inlines:
            if line.startswith("server.") and hostname + ":" + minusOnePort + ":" + plusOnePort in line:
                # server.1=host1:2187:2189
                matchObj    = re.match("server\.(.*?)=.*", line)
                zkServerId  = matchObj.group(1)

        cmdStr = "ssh " + hostname + " 'mkdir -p " + dataDir + "; echo " + zkServerId + " > " + dataDir + "/myid'"
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        subproc = system_test_utils.sys_call_return_subproc(cmdStr)
        for line in subproc.stdout.readlines():
            pass    # dummy loop to wait until producer is completed

        time.sleep(2)
        start_entity_in_background(systemTestEnv, testcaseEnv, zkEntityId)

def start_brokers(systemTestEnv, testcaseEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    brokerEntityIdList = system_test_utils.get_data_from_list_of_dicts(
        clusterEntityConfigDictList, "role", "broker", "entity_id")

    for brokerEntityId in brokerEntityIdList:
        start_entity_in_background(systemTestEnv, testcaseEnv, brokerEntityId)

def start_console_consumers(systemTestEnv, testcaseEnv, onlyThisEntityId=None):

    if onlyThisEntityId is not None:
        start_entity_in_background(systemTestEnv, testcaseEnv, onlyThisEntityId)
    else:
        clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
        consoleConsumerEntityIdList = system_test_utils.get_data_from_list_of_dicts(
            clusterEntityConfigDictList, "role", "console_consumer", "entity_id")
        for entityId in consoleConsumerEntityIdList:
            start_entity_in_background(systemTestEnv, testcaseEnv, entityId)


def start_mirror_makers(systemTestEnv, testcaseEnv, onlyThisEntityId=None):

    if onlyThisEntityId is not None:
        start_entity_in_background(systemTestEnv, testcaseEnv, onlyThisEntityId)
    else:
        clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
        brokerEntityIdList          = system_test_utils.get_data_from_list_of_dicts(
                                      clusterEntityConfigDictList, "role", "mirror_maker", "entity_id")

        for brokerEntityId in brokerEntityIdList:
            start_entity_in_background(systemTestEnv, testcaseEnv, brokerEntityId)


def get_broker_shutdown_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict):

    logger.info("looking up broker shutdown...", extra=d)

    # keep track of broker related data in this dict such as broker id,
    # entity id and timestamp and return it to the caller function
    shutdownBrokerDict = {}

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    brokerEntityIdList = system_test_utils.get_data_from_list_of_dicts(
                             clusterEntityConfigDictList, "role", "broker", "entity_id")

    for brokerEntityId in brokerEntityIdList:

        hostname   = system_test_utils.get_data_by_lookup_keyval(
                         clusterEntityConfigDictList, "entity_id", brokerEntityId, "hostname")
        logFile    = system_test_utils.get_data_by_lookup_keyval(
                         testcaseEnv.testcaseConfigsList, "entity_id", brokerEntityId, "log_filename")

        logPathName = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")
        cmdStrList = ["ssh " + hostname,
                      "\"grep -i -h '" + leaderAttributesDict["BROKER_SHUT_DOWN_COMPLETED_MSG"] + "' ",
                      logPathName + "/" + logFile + " | ",
                      "sort | tail -1\""]
        cmdStr     = " ".join(cmdStrList)

        logger.debug("executing command [" + cmdStr + "]", extra=d)
        subproc = system_test_utils.sys_call_return_subproc(cmdStr)
        for line in subproc.stdout.readlines():

            line = line.rstrip('\n')

            if leaderAttributesDict["BROKER_SHUT_DOWN_COMPLETED_MSG"] in line:
                logger.debug("found the log line : " + line, extra=d)
                try:
                    matchObj    = re.match(leaderAttributesDict["REGX_BROKER_SHUT_DOWN_COMPLETED_PATTERN"], line)
                    datetimeStr = matchObj.group(1)
                    datetimeObj = datetime.strptime(datetimeStr, "%Y-%m-%d %H:%M:%S,%f")
                    unixTs = time.mktime(datetimeObj.timetuple()) + 1e-6*datetimeObj.microsecond
                    #print "{0:.3f}".format(unixTs)

                    # update shutdownBrokerDict when
                    # 1. shutdownBrokerDict has no logline entry
                    # 2. shutdownBrokerDict has existing logline enty but found another logline with more recent timestamp
                    if (len(shutdownBrokerDict) > 0 and shutdownBrokerDict["timestamp"] < unixTs) or (len(shutdownBrokerDict) == 0):
                        shutdownBrokerDict["timestamp"] = unixTs
                        shutdownBrokerDict["brokerid"]  = matchObj.group(2)
                        shutdownBrokerDict["hostname"]  = hostname
                        shutdownBrokerDict["entity_id"] = brokerEntityId
                    logger.debug("brokerid: [" + shutdownBrokerDict["brokerid"] + \
                        "] entity_id: [" + shutdownBrokerDict["entity_id"] + "]", extra=d)
                except:
                    logger.error("ERROR [unable to find matching leader details: Has the matching pattern changed?]", extra=d)
                    raise

    return shutdownBrokerDict


def get_leader_elected_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict):

    logger.debug("looking up leader...", extra=d)

    # keep track of leader related data in this dict such as broker id,
    # entity id and timestamp and return it to the caller function
    leaderDict = {}

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    brokerEntityIdList = system_test_utils.get_data_from_list_of_dicts( \
                             clusterEntityConfigDictList, "role", "broker", "entity_id")

    for brokerEntityId in brokerEntityIdList:

        hostname   = system_test_utils.get_data_by_lookup_keyval( \
                         clusterEntityConfigDictList, "entity_id", brokerEntityId, "hostname")
        kafkaHome  = system_test_utils.get_data_by_lookup_keyval( \
                         clusterEntityConfigDictList, "entity_id", brokerEntityId, "kafka_home")
        logFile    = system_test_utils.get_data_by_lookup_keyval( \
                         testcaseEnv.testcaseConfigsList, "entity_id", brokerEntityId, "log_filename")

        logPathName = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")

        if hostname != "localhost":
            logPathName = replace_kafka_home(logPathName, kafkaHome)

        cmdStrList = ["ssh " + hostname,
                      "\"grep -i -h '" + leaderAttributesDict["LEADER_ELECTION_COMPLETED_MSG"] + "' ",
                      logPathName + "/" + logFile + " | ",
                      "sort | tail -1\""]
        cmdStr     = " ".join(cmdStrList)

        logger.debug("executing command [" + cmdStr + "]", extra=d)
        subproc = system_test_utils.sys_call_return_subproc(cmdStr)
        for line in subproc.stdout.readlines():

            line = line.rstrip('\n')

            if leaderAttributesDict["LEADER_ELECTION_COMPLETED_MSG"] in line:
                logger.debug("found the log line : " + line, extra=d)
                try:
                    matchObj    = re.match(leaderAttributesDict["REGX_LEADER_ELECTION_PATTERN"], line)
                    datetimeStr = matchObj.group(1)
                    datetimeObj = datetime.strptime(datetimeStr, "%Y-%m-%d %H:%M:%S,%f")
                    unixTs = time.mktime(datetimeObj.timetuple()) + 1e-6*datetimeObj.microsecond
                    #print "{0:.3f}".format(unixTs)

                    # update leaderDict when
                    # 1. leaderDict has no logline entry
                    # 2. leaderDict has existing logline entry but found another logline with more recent timestamp
                    if (len(leaderDict) > 0 and leaderDict["timestamp"] < unixTs) or (len(leaderDict) == 0):
                        leaderDict["timestamp"] = unixTs
                        leaderDict["brokerid"]  = matchObj.group(2)
                        leaderDict["topic"]     = matchObj.group(3)
                        leaderDict["partition"] = matchObj.group(4)
                        leaderDict["entity_id"] = brokerEntityId
                        leaderDict["hostname"]  = hostname
                    logger.debug("brokerid: [" + leaderDict["brokerid"] + "] entity_id: [" + leaderDict["entity_id"] + "]", extra=d)
                except:
                    logger.error("ERROR [unable to find matching leader details: Has the matching pattern changed?]", extra=d)
                    raise
            #else:
            #    logger.debug("unmatched line found [" + line + "]", extra=d)

    return leaderDict


def start_entity_in_background(systemTestEnv, testcaseEnv, entityId):

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    # cluster configurations:
    hostname  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "hostname")
    role      = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "role")
    kafkaHome = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "kafka_home")
    javaHome  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "java_home")
    jmxPort   = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "jmx_port")
    clusterName = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "cluster_name")

    # testcase configurations:
    testcaseConfigsList = testcaseEnv.testcaseConfigsList
    clientPort = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "clientPort")
    configFile = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "config_filename")
    logFile    = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "log_filename")

    useNewProducer = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "new-producer")
    mmConsumerConfigFile = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId,
                           "mirror_consumer_config_filename")
    mmProducerConfigFile = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId,
                           "mirror_producer_config_filename")

    logger.info("starting " + role + " in host [" + hostname + "] on client port [" + clientPort + "]", extra=d)

    configPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "config")
    logPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "default")

    if hostname != "localhost":
        configPathName = replace_kafka_home(configPathName, kafkaHome)
        logPathName    = replace_kafka_home(logPathName, kafkaHome)

    if role == "zookeeper":
        cmdList = ["ssh " + hostname,
                  "'JAVA_HOME=" + javaHome,
                  "JMX_PORT=" + jmxPort,
                  kafkaHome + "/bin/zookeeper-server-start.sh ",
                  configPathName + "/" + configFile + " &> ",
                  logPathName + "/" + logFile + " & echo pid:$! > ",
                  logPathName + "/entity_" + entityId + "_pid'"]

    elif role == "broker":
        cmdList = ["ssh " + hostname,
                  "'JAVA_HOME=" + javaHome,
                  "JMX_PORT=" + jmxPort,
                  "KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%s/config/log4j.properties" % kafkaHome,
                  kafkaHome + "/bin/kafka-run-class.sh kafka.Kafka",
                  configPathName + "/" + configFile + " >> ",
                  logPathName + "/" + logFile + " & echo pid:$! > ",
                  logPathName + "/entity_" + entityId + "_pid'"]

    elif role == "mirror_maker":
        if useNewProducer.lower() == "true":
            cmdList = ["ssh " + hostname,
                      "'JAVA_HOME=" + javaHome,
                      "JMX_PORT=" + jmxPort,
                      kafkaHome + "/bin/kafka-run-class.sh kafka.tools.MirrorMaker",
                      "--consumer.config " + configPathName + "/" + mmConsumerConfigFile,
                      "--producer.config " + configPathName + "/" + mmProducerConfigFile,
                      "--new.producer",
                      "--whitelist=\".*\" >> ",
                      logPathName + "/" + logFile + " & echo pid:$! > ",
                      logPathName + "/entity_" + entityId + "_pid'"]
        else:
            cmdList = ["ssh " + hostname,
                      "'JAVA_HOME=" + javaHome,
                      "JMX_PORT=" + jmxPort,
                      kafkaHome + "/bin/kafka-run-class.sh kafka.tools.MirrorMaker",
                      "--consumer.config " + configPathName + "/" + mmConsumerConfigFile,
                      "--producer.config " + configPathName + "/" + mmProducerConfigFile,
                      "--whitelist=\".*\" >> ",
                      logPathName + "/" + logFile + " & echo pid:$! > ",
                      logPathName + "/entity_" + entityId + "_pid'"]

    elif role == "console_consumer":
        clusterToConsumeFrom = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "cluster_name")
        numTopicsForAutoGenString = -1
        try:
            numTopicsForAutoGenString = int(testcaseEnv.testcaseArgumentsDict["num_topics_for_auto_generated_string"])
        except:
            pass

        topic = ""
        if numTopicsForAutoGenString < 0:
            topic = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "topic")
        else:
            topic = generate_topics_string("topic", numTopicsForAutoGenString)

        # update this variable and will be used by data validation functions
        testcaseEnv.consumerTopicsString = topic

        # 2. consumer timeout
        timeoutMs = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "consumer-timeout-ms")

        # 3. consumer formatter
        formatterOption = ""
        try:
            formatterOption = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "formatter")
        except:
            pass

        # 4. consumer config
        consumerProperties = {}
        consumerProperties["consumer.timeout.ms"] = timeoutMs
        try:
            groupOption = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "group.id")
            consumerProperties["group.id"] = groupOption
        except:
            pass

        props_file_path=write_consumer_properties(consumerProperties)
        scpCmdStr = "scp "+ props_file_path +" "+ hostname + ":/tmp/"
        logger.debug("executing command [" + scpCmdStr + "]", extra=d)
        system_test_utils.sys_call(scpCmdStr)

        if len(formatterOption) > 0:
            formatterOption = " --formatter " + formatterOption + " "

        # get zookeeper connect string
        zkConnectStr = ""
        if clusterName == "source":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
        elif clusterName == "target":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
        else:
            logger.error("Invalid cluster name : " + clusterName, extra=d)
            sys.exit(1)
        cmdList = ["ssh " + hostname,
                   "'JAVA_HOME=" + javaHome,
                   "JMX_PORT=" + jmxPort,
                   kafkaHome + "/bin/kafka-run-class.sh kafka.tools.ConsoleConsumer",
                   "--zookeeper " + zkConnectStr,
                   "--topic " + topic,
                   "--consumer.config /tmp/consumer.properties",
                   "--csv-reporter-enabled",
                   formatterOption,
                   "--from-beginning",
                   " >> " + logPathName + "/" + logFile + " & echo pid:$! > ",
                   logPathName + "/entity_" + entityId + "_pid'"]

    cmdStr = " ".join(cmdList)

    logger.debug("executing command: [" + cmdStr + "]", extra=d)
    system_test_utils.async_sys_call(cmdStr)
    logger.info("sleeping for 5 seconds.", extra=d)
    time.sleep(5)

    pidCmdStr = "ssh " + hostname + " 'cat " + logPathName + "/entity_" + entityId + "_pid' 2> /dev/null"
    logger.debug("executing command: [" + pidCmdStr + "]", extra=d)
    subproc = system_test_utils.sys_call_return_subproc(pidCmdStr)

    # keep track of the remote entity pid in a dictionary
    for line in subproc.stdout.readlines():
        if line.startswith("pid"):
            line = line.rstrip('\n')
            logger.debug("found pid line: [" + line + "]", extra=d)
            tokens = line.split(':')
            if role == "zookeeper":
                testcaseEnv.entityZkParentPidDict[entityId] = tokens[1]
            elif role == "broker":
                testcaseEnv.entityBrokerParentPidDict[entityId] = tokens[1]
            elif role == "mirror_maker":
                testcaseEnv.entityMirrorMakerParentPidDict[entityId] = tokens[1]
            elif role == "console_consumer":
                testcaseEnv.entityConsoleConsumerParentPidDict[entityId] = tokens[1]


def start_console_consumer(systemTestEnv, testcaseEnv):

    clusterList = systemTestEnv.clusterEntityConfigDictList

    consumerConfigList = system_test_utils.get_dict_from_list_of_dicts(clusterList, "role", "console_consumer")
    for consumerConfig in consumerConfigList:
        host              = consumerConfig["hostname"]
        entityId          = consumerConfig["entity_id"]
        jmxPort           = consumerConfig["jmx_port"]
        role              = consumerConfig["role"]
        clusterName       = consumerConfig["cluster_name"]
        kafkaHome         = system_test_utils.get_data_by_lookup_keyval(clusterList, "entity_id", entityId, "kafka_home")
        javaHome          = system_test_utils.get_data_by_lookup_keyval(clusterList, "entity_id", entityId, "java_home")
        jmxPort           = system_test_utils.get_data_by_lookup_keyval(clusterList, "entity_id", entityId, "jmx_port")
        kafkaRunClassBin  = kafkaHome + "/bin/kafka-run-class.sh"

        logger.info("starting console consumer", extra=d)

        consumerLogPath = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", entityId, "default")
        metricsDir      = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", entityId, "metrics"),

        if host != "localhost":
            consumerLogPath = replace_kafka_home(consumerLogPath, kafkaHome)
            #metricsDir      = replace_kafka_home(metricsDir, kafkaHome)

        consumerLogPathName = consumerLogPath + "/console_consumer.log"

        testcaseEnv.userDefinedEnvVarDict["consumerLogPathName"] = consumerLogPathName

        # testcase configurations:
        testcaseList = testcaseEnv.testcaseConfigsList

        # get testcase arguments
        # 1. topics
        numTopicsForAutoGenString = -1
        try:
            numTopicsForAutoGenString = int(testcaseEnv.testcaseArgumentsDict["num_topics_for_auto_generated_string"])
        except:
            pass

        topic = ""
        if numTopicsForAutoGenString < 0:
            topic = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "topic")
        else:
            topic = generate_topics_string("topic", numTopicsForAutoGenString)

        # update this variable and will be used by data validation functions
        testcaseEnv.consumerTopicsString = topic

        # 2. consumer timeout
        timeoutMs = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "consumer-timeout-ms")

        # 3. consumer formatter
        formatterOption = ""
        try:
            formatterOption = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "formatter")
        except:
            pass

        if len(formatterOption) > 0:
            formatterOption = " --formatter " + formatterOption + " "

        # get zookeeper connect string
        zkConnectStr = ""
        if clusterName == "source":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
        elif clusterName == "target":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
        else:
            logger.error("Invalid cluster name : " + clusterName, extra=d)
            sys.exit(1)

        consumerProperties = {}
        consumerProperties["consumer.timeout.ms"] = timeoutMs
        props_file_path=write_consumer_properties(consumerProperties)
        scpCmdStr = "scp "+ props_file_path +" "+ host + ":/tmp/"
        logger.debug("executing command [" + scpCmdStr + "]", extra=d)
        system_test_utils.sys_call(scpCmdStr)

        cmdList = ["ssh " + host,
                   "'JAVA_HOME=" + javaHome,
                   "JMX_PORT=" + jmxPort,
                   kafkaRunClassBin + " kafka.tools.ConsoleConsumer",
                   "--zookeeper " + zkConnectStr,
                   "--topic " + topic,
                   "--consumer.config /tmp/consumer.properties",
                   "--csv-reporter-enabled",
                   #"--metrics-dir " + metricsDir,
                   formatterOption,
                   "--from-beginning ",
                   " >> " + consumerLogPathName,
                   " & echo pid:$! > " + consumerLogPath + "/entity_" + entityId + "_pid'"]

        cmdStr = " ".join(cmdList)

        logger.debug("executing command: [" + cmdStr + "]", extra=d)
        system_test_utils.async_sys_call(cmdStr)

        pidCmdStr = "ssh " + host + " 'cat " + consumerLogPath + "/entity_" + entityId + "_pid'"
        logger.debug("executing command: [" + pidCmdStr + "]", extra=d)
        subproc = system_test_utils.sys_call_return_subproc(pidCmdStr)

        # keep track of the remote entity pid in a dictionary
        for line in subproc.stdout.readlines():
            if line.startswith("pid"):
                line = line.rstrip('\n')
                logger.debug("found pid line: [" + line + "]", extra=d)
                tokens = line.split(':')
                testcaseEnv.consumerHostParentPidDict[host] = tokens[1]

def start_producer_performance(systemTestEnv, testcaseEnv, kafka07Client):

    entityConfigList     = systemTestEnv.clusterEntityConfigDictList
    testcaseConfigsList  = testcaseEnv.testcaseConfigsList
    brokerListStr = ""

    # construct "broker-list" for producer
    for entityConfig in entityConfigList:
        entityRole = entityConfig["role"]
        if entityRole == "broker":
            hostname = entityConfig["hostname"]
            entityId = entityConfig["entity_id"]
            port     = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "port")

    producerConfigList = system_test_utils.get_dict_from_list_of_dicts(entityConfigList, "role", "producer_performance")
    for producerConfig in producerConfigList:
        host              = producerConfig["hostname"]
        entityId          = producerConfig["entity_id"]
        jmxPort           = producerConfig["jmx_port"]
        role              = producerConfig["role"]

        thread.start_new_thread(start_producer_in_thread, (testcaseEnv, entityConfigList, producerConfig, kafka07Client))
        logger.debug("calling testcaseEnv.lock.acquire()", extra=d)
        testcaseEnv.lock.acquire()
        testcaseEnv.numProducerThreadsRunning += 1
        logger.debug("testcaseEnv.numProducerThreadsRunning : " + str(testcaseEnv.numProducerThreadsRunning), extra=d)
        time.sleep(1)
        logger.debug("calling testcaseEnv.lock.release()", extra=d)
        testcaseEnv.lock.release()

def generate_topics_string(topicPrefix, numOfTopics):
    # return a topics string in the following format:
    # <topicPrefix>_0001,<topicPrefix>_0002,...
    # eg. "topic_0001,topic_0002,...,topic_xxxx"

    topicsStr = ""
    counter   = 1
    idx       = "1"
    while counter <= numOfTopics:
        if counter <= 9:
            idx = "000" + str(counter)
        elif counter <= 99:
            idx = "00"  + str(counter)
        elif counter <= 999:
            idx = "0"  +  str(counter)
        elif counter <= 9999:
            idx = str(counter)
        else:
            raise Exception("Error: no. of topics must be under 10000 - current topics count : " + counter)

        if len(topicsStr) == 0:
            topicsStr = topicPrefix + "_" + idx
        else:
            topicsStr = topicsStr + "," + topicPrefix + "_" + idx

        counter += 1
    return topicsStr

def start_producer_in_thread(testcaseEnv, entityConfigList, producerConfig, kafka07Client):
    host              = producerConfig["hostname"]
    entityId          = producerConfig["entity_id"]
    jmxPort           = producerConfig["jmx_port"]
    role              = producerConfig["role"]
    clusterName       = producerConfig["cluster_name"]
    kafkaHome         = system_test_utils.get_data_by_lookup_keyval(entityConfigList, "entity_id", entityId, "kafka_home")
    javaHome          = system_test_utils.get_data_by_lookup_keyval(entityConfigList, "entity_id", entityId, "java_home")
    jmxPort           = system_test_utils.get_data_by_lookup_keyval(entityConfigList, "entity_id", entityId, "jmx_port")
    kafkaRunClassBin  = kafkaHome + "/bin/kafka-run-class.sh"

    # first keep track of its pid
    testcaseEnv.producerHostParentPidDict[entityId] = os.getpid()

    # get optional testcase arguments
    numTopicsForAutoGenString = -1
    try:
        numTopicsForAutoGenString = int(testcaseEnv.testcaseArgumentsDict["num_topics_for_auto_generated_string"])
    except:
        pass

    # testcase configurations:
    testcaseConfigsList = testcaseEnv.testcaseConfigsList
    topic = ""
    if numTopicsForAutoGenString < 0:
        topic      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "topic")
    else:
        topic      = generate_topics_string("topic", numTopicsForAutoGenString)

    # update this variable and will be used by data validation functions
    testcaseEnv.producerTopicsString = topic

    threads        = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "threads")
    compCodec      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "compression-codec")
    messageSize    = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "message-size")
    noMsgPerBatch  = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "message")
    requestNumAcks = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "request-num-acks")
    syncMode       = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "sync")
    useNewProducer = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "new-producer")
    retryBackoffMs = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "producer-retry-backoff-ms")
    numOfRetries   = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "producer-num-retries")

    # for optional properties in testcase_xxxx_properties.json,
    # check the length of returned value for those properties:
    if len(retryBackoffMs) == 0:      # no setting for "producer-retry-backoff-ms"
        retryBackoffMs     =  "100"   # default
    if len(numOfRetries)   == 0:      # no setting for "producer-num-retries"
        numOfRetries       =  "3"     # default

    brokerListStr  = ""
    if clusterName == "source":
        brokerListStr  = testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"]
    elif clusterName == "target":
        brokerListStr  = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]
    else:
        logger.error("Unknown cluster name: " + clusterName, extra=d)
        sys.exit(1)

    logger.info("starting producer preformance", extra=d)

    producerLogPath  = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", entityId, "default")
    metricsDir       = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", entityId, "metrics")

    if host != "localhost":
        producerLogPath = replace_kafka_home(producerLogPath, kafkaHome)
        metricsDir      = replace_kafka_home(metricsDir, kafkaHome)

    producerLogPathName = producerLogPath + "/producer_performance.log"

    testcaseEnv.userDefinedEnvVarDict["producerLogPathName"] = producerLogPathName

    counter = 0
    producerSleepSec = int(testcaseEnv.testcaseArgumentsDict["sleep_seconds_between_producer_calls"])

    boolArgumentsStr = ""
    if syncMode.lower() == "true":
        boolArgumentsStr = boolArgumentsStr + " --sync"
    if useNewProducer.lower() == "true":
        boolArgumentsStr = boolArgumentsStr + " --new-producer"

    # keep calling producer until signaled to stop by:
    # testcaseEnv.userDefinedEnvVarDict["stopBackgroundProducer"]
    while 1:
        logger.debug("calling testcaseEnv.lock.acquire()", extra=d)
        testcaseEnv.lock.acquire()
        if not testcaseEnv.userDefinedEnvVarDict["stopBackgroundProducer"]:
            initMsgId = counter * int(noMsgPerBatch)

            logger.info("#### [producer thread] status of stopBackgroundProducer : [False] => producing [" \
                + str(noMsgPerBatch) + "] messages with starting message id : [" + str(initMsgId) + "]", extra=d)

            cmdList = ["ssh " + host,
                       "'JAVA_HOME=" + javaHome,
                       "JMX_PORT=" + jmxPort,
                       "KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%s/config/test-log4j.properties" % kafkaHome,
                       kafkaRunClassBin + " kafka.tools.ProducerPerformance",
                       "--broker-list " + brokerListStr,
                       "--initial-message-id " + str(initMsgId),
                       "--messages " + noMsgPerBatch,
                       "--topics " + topic,
                       "--threads " + threads,
                       "--compression-codec " + compCodec,
                       "--message-size " + messageSize,
                       "--request-num-acks " + requestNumAcks,
                       "--producer-retry-backoff-ms " + retryBackoffMs,
                       "--producer-num-retries " + numOfRetries,
                       "--csv-reporter-enabled",
                       "--metrics-dir " + metricsDir,
                       boolArgumentsStr,
                       " >> " + producerLogPathName,
                       " & echo $! > " + producerLogPath + "/entity_" + entityId + "_pid",
                       " & wait'"]

            if kafka07Client:
                cmdList[:] = []

                brokerInfoStr = ""
                tokenList = brokerListStr.split(',')
                index = 1
                for token in tokenList:
                    if len(brokerInfoStr) == 0:
                        brokerInfoStr = str(index) + ":" + token
                    else:
                        brokerInfoStr += "," + str(index) + ":" + token
                    index += 1

                brokerInfoStr = "broker.list=" + brokerInfoStr

                cmdList = ["ssh " + host,
                       "'JAVA_HOME=" + javaHome,
                       "JMX_PORT=" + jmxPort,
                       "KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%s/config/test-log4j.properties" % kafkaHome,
                       kafkaRunClassBin + " kafka.tools.ProducerPerformance",
                       "--brokerinfo " + brokerInfoStr,
                       "--initial-message-id " + str(initMsgId),
                       "--messages " + noMsgPerBatch,
                       "--topic " + topic,
                       "--threads " + threads,
                       "--compression-codec " + compCodec,
                       "--message-size " + messageSize,
                       "--vary-message-size --async",
                       " >> " + producerLogPathName,
                       " & echo $! > " + producerLogPath + "/entity_" + entityId + "_pid",
                       " & wait'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)

            subproc = system_test_utils.sys_call_return_subproc(cmdStr)
            logger.debug("waiting for producer to finish", extra=d)
            subproc.communicate()
            logger.debug("producer finished", extra=d)
        else:
            testcaseEnv.numProducerThreadsRunning -= 1
            logger.debug("testcaseEnv.numProducerThreadsRunning : " + str(testcaseEnv.numProducerThreadsRunning), extra=d)
            logger.debug("calling testcaseEnv.lock.release()", extra=d)
            testcaseEnv.lock.release()
            break

        counter += 1
        logger.debug("calling testcaseEnv.lock.release()", extra=d)
        testcaseEnv.lock.release()
        time.sleep(int(producerSleepSec))

    # wait until other producer threads also stops and
    # let the main testcase know all producers have stopped
    while 1:
        logger.debug("calling testcaseEnv.lock.acquire()", extra=d)
        testcaseEnv.lock.acquire()
        time.sleep(1)
        if testcaseEnv.numProducerThreadsRunning == 0:
            testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"] = True
            logger.debug("calling testcaseEnv.lock.release()", extra=d)
            testcaseEnv.lock.release()
            break
        else:
            logger.debug("waiting for TRUE of testcaseEnv.userDefinedEnvVarDict['backgroundProducerStopped']", extra=d)
            logger.debug("calling testcaseEnv.lock.release()", extra=d)
            testcaseEnv.lock.release()
        time.sleep(1)

    # finally remove itself from the tracking pids
    del testcaseEnv.producerHostParentPidDict[entityId]

def stop_remote_entity(systemTestEnv, entityId, parentPid, signalType="SIGTERM"):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    hostname  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "hostname")
    pidStack  = system_test_utils.get_remote_child_processes(hostname, parentPid)

    logger.info("terminating (" + signalType + ") process id: " + parentPid + " in host: " + hostname, extra=d)

    if signalType.lower() == "sigterm":
        system_test_utils.sigterm_remote_process(hostname, pidStack)
    elif signalType.lower() == "sigkill":
        system_test_utils.sigkill_remote_process(hostname, pidStack)
    else:
        logger.error("Invalid signal type: " + signalType, extra=d)
        raise Exception("Invalid signal type: " + signalType)


def force_stop_remote_entity(systemTestEnv, entityId, parentPid):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    hostname  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "hostname")
    pidStack  = system_test_utils.get_remote_child_processes(hostname, parentPid)

    logger.debug("terminating process id: " + parentPid + " in host: " + hostname, extra=d)
    system_test_utils.sigkill_remote_process(hostname, pidStack)


def create_topic_for_producer_performance(systemTestEnv, testcaseEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")

    for prodPerfCfg in prodPerfCfgList:
        topicsStr       = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", prodPerfCfg["entity_id"], "topic")
        zkEntityId      = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "entity_id")
        zkHost          = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "hostname")
        kafkaHome       = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "kafka_home")
        javaHome        = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "java_home")
        createTopicBin  = kafkaHome + "/bin/kafka-topics.sh --create"

        logger.debug("zkEntityId     : " + zkEntityId, extra=d)
        logger.debug("createTopicBin : " + createTopicBin, extra=d)

        zkConnectStr = ""
        topicsList   = topicsStr.split(',')

        if len(testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]) > 0:
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
        elif len(testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]) > 0:
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
        else:
            raise Exception("Empty zkConnectStr found")

        testcaseBaseDir = testcaseEnv.testCaseBaseDir

        if zkHost != "localhost":
            testcaseBaseDir = replace_kafka_home(testcaseBaseDir, kafkaHome)

        for topic in topicsList:
            logger.info("creating topic: [" + topic + "] at: [" + zkConnectStr + "]", extra=d)
            cmdList = ["ssh " + zkHost,
                       "'JAVA_HOME=" + javaHome,
                       createTopicBin,
                       " --topic "     + topic,
                       " --zookeeper " + zkConnectStr,
                       " --replication-factor "   + testcaseEnv.testcaseArgumentsDict["replica_factor"],
                       " --partitions " + testcaseEnv.testcaseArgumentsDict["num_partition"] + " >> ",
                       testcaseBaseDir + "/logs/create_source_cluster_topic.log'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)
            subproc = system_test_utils.sys_call_return_subproc(cmdStr)

def create_topic(systemTestEnv, testcaseEnv, topic, replication_factor, num_partitions):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    zkEntityId      = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "entity_id")
    kafkaHome       = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "kafka_home")
    javaHome        = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "java_home")
    createTopicBin  = kafkaHome + "/bin/kafka-topics.sh --create"
    zkConnectStr = ""
    zkHost = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "hostname")
    if len(testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]) > 0:
        zkConnectStr = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
    elif len(testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]) > 0:
        zkConnectStr = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
    else:
        raise Exception("Empty zkConnectStr found")

    testcaseBaseDir = testcaseEnv.testCaseBaseDir

    testcaseBaseDir = replace_kafka_home(testcaseBaseDir, kafkaHome)

    logger.debug("creating topic: [" + topic + "] at: [" + zkConnectStr + "]", extra=d)
    cmdList = ["ssh " + zkHost,
               "'JAVA_HOME=" + javaHome,
               createTopicBin,
               " --topic "     + topic,
               " --zookeeper " + zkConnectStr,
               " --replication-factor "   + str(replication_factor),
               " --partitions " + str(num_partitions) + " >> ",
               testcaseBaseDir + "/logs/create_source_cluster_topic.log'"]

    cmdStr = " ".join(cmdList)
    logger.info("executing command: [" + cmdStr + "]", extra=d)
    subproc = system_test_utils.sys_call_return_subproc(cmdStr)



def get_message_id(logPathName, topic=""):
    logLines      = open(logPathName, "r").readlines()
    messageIdList = []

    for line in logLines:
        if not "MessageID" in line:
            continue
        else:
            matchObj = re.match('.*Topic:(.*?):.*:MessageID:(.*?):', line)
            if len(topic) == 0:
                messageIdList.append( matchObj.group(2) )
            else:
                if topic == matchObj.group(1):
                    messageIdList.append( matchObj.group(2) )

    return messageIdList

def get_message_checksum(logPathName):
    logLines = open(logPathName, "r").readlines()
    messageChecksumList = []

    for line in logLines:
        if not "checksum:" in line:
            continue
        else:
            matchObj = re.match('.*checksum:(\d*).*', line)
            if matchObj is not None:
                checksum = matchObj.group(1)
                messageChecksumList.append( checksum )
            else:
                logger.error("unexpected log line : " + line, extra=d)

    return messageChecksumList


def validate_data_matched(systemTestEnv, testcaseEnv, replicationUtils):
    logger.info("#### Inside validate_data_matched", extra=d)

    validationStatusDict        = testcaseEnv.validationStatusDict
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")
    consumerCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "console_consumer")

    consumerDuplicateCount = 0

    for prodPerfCfg in prodPerfCfgList:
        producerEntityId = prodPerfCfg["entity_id"]
        topic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", producerEntityId, "topic")
        logger.debug("working on topic : " + topic, extra=d)
        acks  = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", producerEntityId, "request-num-acks")

        consumerEntityIdList = system_test_utils.get_data_from_list_of_dicts( \
                           clusterEntityConfigDictList, "role", "console_consumer", "entity_id")

        matchingConsumerEntityId = None
        for consumerEntityId in consumerEntityIdList:
            consumerTopic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", consumerEntityId, "topic")
            if consumerTopic in topic:
                matchingConsumerEntityId = consumerEntityId
                logger.info("matching consumer entity id found", extra=d)
                break

        if matchingConsumerEntityId is None:
            logger.info("matching consumer entity id NOT found", extra=d)
            break

        msgIdMissingInConsumerLogPathName = get_testcase_config_log_dir_pathname( \
                           testcaseEnv, "console_consumer", matchingConsumerEntityId, "default") + "/msg_id_missing_in_consumer.log"
        producerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", producerEntityId, "default")
        producerLogPathName = producerLogPath + "/producer_performance.log"

        consumerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", matchingConsumerEntityId, "default")
        consumerLogPathName = consumerLogPath + "/console_consumer.log"

        producerMsgIdList  = get_message_id(producerLogPathName)
        consumerMsgIdList  = get_message_id(consumerLogPathName)
        producerMsgIdSet   = set(producerMsgIdList)
        consumerMsgIdSet   = set(consumerMsgIdList)

        consumerDuplicateCount = len(consumerMsgIdList) - len(consumerMsgIdSet)
        missingUniqConsumerMsgId = system_test_utils.subtract_list(producerMsgIdSet, consumerMsgIdSet)

        outfile = open(msgIdMissingInConsumerLogPathName, "w")
        for id in missingUniqConsumerMsgId:
            outfile.write(id + "\n")
        outfile.close()

        logger.info("no. of unique messages on topic [" + topic + "] sent from publisher  : " + str(len(producerMsgIdSet)), extra=d)
        logger.info("no. of unique messages on topic [" + topic + "] received by consumer : " + str(len(consumerMsgIdSet)), extra=d)
        validationStatusDict["Unique messages from producer on [" + topic + "]"] = str(len(producerMsgIdSet))
        validationStatusDict["Unique messages from consumer on [" + topic + "]"] = str(len(consumerMsgIdSet))

        missingPercentage = len(missingUniqConsumerMsgId) * 100.00 / len(producerMsgIdSet)
        logger.info("Data loss threshold % : " + str(replicationUtils.ackOneDataLossThresholdPercent), extra=d)
        logger.warn("Data loss % on topic : " + topic + " : " + str(missingPercentage), extra=d)

        if ( len(missingUniqConsumerMsgId) == 0 and len(producerMsgIdSet) > 0 ):
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
        elif (acks == "1"):
            if missingPercentage <= replicationUtils.ackOneDataLossThresholdPercent:
                validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
                logger.warn("Test case (Acks = 1) passes with less than " + str(replicationUtils.ackOneDataLossThresholdPercent) \
                    + "% data loss : [" + str(len(missingUniqConsumerMsgId)) + "] missing messages", extra=d)
            else:
                validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
                logger.error("Test case (Acks = 1) failed with more than " + str(replicationUtils.ackOneDataLossThresholdPercent) \
                    + "% data loss : [" + str(len(missingUniqConsumerMsgId)) + "] missing messages", extra=d)
        else:
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
            logger.info("See " + msgIdMissingInConsumerLogPathName + " for missing MessageID", extra=d)


def validate_leader_election_successful(testcaseEnv, leaderDict, validationStatusDict):
    logger.debug("#### Inside validate_leader_election_successful", extra=d)

    if ( len(leaderDict) > 0 ):
        try:
            leaderBrokerId = leaderDict["brokerid"]
            leaderEntityId = leaderDict["entity_id"]
            leaderPid      = testcaseEnv.entityBrokerParentPidDict[leaderEntityId]
            hostname       = leaderDict["hostname"]

            logger.info("found leader in entity [" + leaderEntityId + "] with brokerid [" + \
                leaderBrokerId + "] for partition [" + leaderDict["partition"] + "]", extra=d)
            validationStatusDict["Validate leader election successful"] = "PASSED"
            return True
        except Exception, e:
            logger.error("leader info not completed: {0}".format(e), extra=d)
            traceback.print_exc()
            print leaderDict
            traceback.print_exc()
            validationStatusDict["Validate leader election successful"] = "FAILED"
            return False
    else:
        validationStatusDict["Validate leader election successful"] = "FAILED"
        return False


def cleanup_data_at_remote_hosts(systemTestEnv, testcaseEnv):

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    testcaseConfigsList         = testcaseEnv.testcaseConfigsList
    testCaseBaseDir             = testcaseEnv.testCaseBaseDir

    # clean up the following directories in localhost
    #     system_test/<xxxx_testsuite>/testcase_xxxx/config
    #     system_test/<xxxx_testsuite>/testcase_xxxx/dashboards
    #     system_test/<xxxx_testsuite>/testcase_xxxx/logs
    logger.info("cleaning up test case dir: [" + testCaseBaseDir + "]", extra=d)

    if "system_test" not in testCaseBaseDir:
        # logger.warn("possible destructive command [" + cmdStr + "]", extra=d)
        logger.warn("check config file: system_test/cluster_config.properties", extra=d)
        logger.warn("aborting test...", extra=d)
        sys.exit(1)
    else:
        system_test_utils.sys_call("rm -rf " + testCaseBaseDir + "/config/*")
        system_test_utils.sys_call("rm -rf " + testCaseBaseDir + "/dashboards/*")
        system_test_utils.sys_call("rm -rf " + testCaseBaseDir + "/logs/*")

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:

        hostname         = clusterEntityConfigDict["hostname"]
        entityId         = clusterEntityConfigDict["entity_id"]
        role             = clusterEntityConfigDict["role"]
        kafkaHome        = clusterEntityConfigDict["kafka_home"]
        cmdStr           = ""
        dataDir          = ""

        if hostname == "localhost":
            remoteTestCaseBaseDir = testCaseBaseDir
        else:
            remoteTestCaseBaseDir = replace_kafka_home(testCaseBaseDir, kafkaHome)

        logger.info("cleaning up data dir on host: [" + hostname + "]", extra=d)

        if role == 'zookeeper':
            dataDir = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "dataDir")
        elif role == 'broker':
            dataDir = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "log.dir")
        else:
            logger.info("skipping role [" + role + "] on host : [" + hostname + "]", extra=d)
            continue

        cmdStr  = "ssh " + hostname + " 'rm -rf " + dataDir + "'"

        if not dataDir.startswith("/tmp"):
            logger.warn("possible destructive command [" + cmdStr + "]", extra=d)
            logger.warn("check config file: system_test/cluster_config.properties", extra=d)
            logger.warn("aborting test...", extra=d)
            sys.exit(1)

        # ============================
        # cleaning data dir
        # ============================
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)

        # ============================
        # cleaning log/metrics/svg, ...
        # ============================
        if system_test_utils.remote_host_file_exists(hostname, kafkaHome + "/bin/kafka-run-class.sh"):
            # so kafkaHome is a real kafka installation
            cmdStr = "ssh " + hostname + " \"find " + remoteTestCaseBaseDir + " -name '*.log' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + remoteTestCaseBaseDir + " -name '*_pid' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + remoteTestCaseBaseDir + " -name '*.csv' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + remoteTestCaseBaseDir + " -name '*.svg' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + remoteTestCaseBaseDir + " -name '*.html' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

def replace_kafka_home(systemTestSubDirPath, kafkaHome):
    matchObj = re.match(".*(\/system_test\/.*)$", systemTestSubDirPath)
    relativeSubDirPath = matchObj.group(1)
    return kafkaHome + relativeSubDirPath

def get_entity_log_directory(testCaseBaseDir, entity_id, role):
    return testCaseBaseDir + "/logs/" + role + "-" + entity_id

def get_entities_for_role(clusterConfig, role):
    return filter(lambda entity: entity['role'] == role, clusterConfig)

def stop_consumer():
    system_test_utils.sys_call("ps -ef | grep ConsoleConsumer | grep -v grep | tr -s ' ' | cut -f2 -d' ' | xargs kill -15")

def ps_grep_terminate_running_entity(systemTestEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    username = getpass.getuser()

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:
        hostname = clusterEntityConfigDict["hostname"]
        cmdList  = ["ssh " + hostname,
                    "\"ps auxw | grep -v grep | grep -v Bootstrap | grep -v vim | grep ^" + username,
                    "| grep -i 'java\|server\-start\|run\-\|producer\|consumer\|jmxtool' | grep kafka",
                    "| tr -s ' ' | cut -f2 -d ' ' | xargs kill -9" + "\""]

        cmdStr = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)

        system_test_utils.sys_call(cmdStr)

def get_reelection_latency(systemTestEnv, testcaseEnv, leaderDict, leaderAttributesDict):
    leaderEntityId = None
    leaderBrokerId = None
    leaderPPid     = None
    shutdownLeaderTimestamp = None
    leaderReElectionLatency = -1

    if testcaseEnv.validationStatusDict["Validate leader election successful"] == "FAILED":
        # leader election is not successful - something is wrong => so skip this testcase
        return None
    else:
        # leader elected => stop leader
        try:
            leaderEntityId = leaderDict["entity_id"]
            leaderBrokerId = leaderDict["brokerid"]
            leaderPPid     = testcaseEnv.entityBrokerParentPidDict[leaderEntityId]
        except:
            logger.info("leader details unavailable", extra=d)
            raise

        logger.info("stopping leader in entity "+leaderEntityId+" with pid "+leaderPPid, extra=d)
        signalType = None
        try:
            signalType = testcaseEnv.testcaseArgumentsDict["signal_type"]
        except:
            pass

        if signalType is None or signalType.lower() == "sigterm":
            stop_remote_entity(systemTestEnv, leaderEntityId, leaderPPid)
        elif signalType.lower() == "sigkill":
            stop_remote_entity(systemTestEnv, leaderEntityId, leaderPPid, "SIGKILL")
        else:
            logger.error("Unsupported signal type: " + signalType, extra=d)
            raise Exception("Unsupported signal type: " + signalType)

    logger.info("sleeping for 10s for leader re-election to complete", extra=d)
    time.sleep(10)

    # get broker shut down completed timestamp
    shutdownBrokerDict = get_broker_shutdown_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict)
    shutdownTimestamp  = -1

    try:
        shutdownTimestamp = shutdownBrokerDict["timestamp"]
        logger.debug("unix timestamp of shut down completed: " + str("{0:.6f}".format(shutdownTimestamp)), extra=d)
    except:
        logger.warn("unable to find broker shut down timestamp", extra=d)

    logger.info("looking up new leader", extra=d)
    leaderDict2 = get_leader_elected_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict)
    logger.debug("unix timestamp of new elected leader: " + str("{0:.6f}".format(leaderDict2["timestamp"])), extra=d)

    if shutdownTimestamp > 0:
        leaderReElectionLatency = float(leaderDict2["timestamp"]) - float(shutdownTimestamp)
        logger.info("leader Re-election Latency: " + str(leaderReElectionLatency) + " sec", extra=d)

    return leaderReElectionLatency


def stop_all_remote_running_processes(systemTestEnv, testcaseEnv):

    entityConfigs = systemTestEnv.clusterEntityConfigDictList

    # If there are any alive local threads that keep starting remote producer performance, we need to kill them;
    # note we do not need to stop remote processes since they will terminate themselves eventually.
    if len(testcaseEnv.producerHostParentPidDict) != 0:
        # =============================================
        # tell producer to stop
        # =============================================
        logger.debug("calling testcaseEnv.lock.acquire()", extra=d)
        testcaseEnv.lock.acquire()
        testcaseEnv.userDefinedEnvVarDict["stopBackgroundProducer"] = True
        logger.debug("calling testcaseEnv.lock.release()", extra=d)
        testcaseEnv.lock.release()

        # =============================================
        # wait for producer thread's update of
        # "backgroundProducerStopped" to be "True"
        # =============================================
        while 1:
            logger.debug("calling testcaseEnv.lock.acquire()", extra=d)
            testcaseEnv.lock.acquire()
            logger.info("status of backgroundProducerStopped : [" + \
                str(testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"]) + "]", extra=d)
            if testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"]:
                logger.debug("calling testcaseEnv.lock.release()", extra=d)
                testcaseEnv.lock.release()
                logger.info("all producer threads completed", extra=d)
                break
            logger.debug("calling testcaseEnv.lock.release()", extra=d)
            testcaseEnv.lock.release()

        testcaseEnv.producerHostParentPidDict.clear()

    for hostname, consumerPPid in testcaseEnv.consumerHostParentPidDict.items():
        consumerEntityId = system_test_utils.get_data_by_lookup_keyval(entityConfigs, "hostname", hostname, "entity_id")
        stop_remote_entity(systemTestEnv, consumerEntityId, consumerPPid)

    for entityId, jmxParentPidList in testcaseEnv.entityJmxParentPidDict.items():
        for jmxParentPid in jmxParentPidList:
            stop_remote_entity(systemTestEnv, entityId, jmxParentPid)

    for entityId, mirrorMakerParentPid in testcaseEnv.entityMirrorMakerParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, mirrorMakerParentPid)

    for entityId, consumerParentPid in testcaseEnv.entityConsoleConsumerParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, consumerParentPid)

    for entityId, brokerParentPid in testcaseEnv.entityBrokerParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, brokerParentPid)

    for entityId, zkParentPid in testcaseEnv.entityZkParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, zkParentPid)


def start_migration_tool(systemTestEnv, testcaseEnv, onlyThisEntityId=None):
    clusterConfigList = systemTestEnv.clusterEntityConfigDictList
    migrationToolConfigList = system_test_utils.get_dict_from_list_of_dicts(clusterConfigList, "role", "migration_tool")

    for migrationToolConfig in migrationToolConfigList:

        entityId = migrationToolConfig["entity_id"]

        if onlyThisEntityId is None or entityId == onlyThisEntityId:

            host              = migrationToolConfig["hostname"]
            jmxPort           = migrationToolConfig["jmx_port"]
            role              = migrationToolConfig["role"]
            kafkaHome         = system_test_utils.get_data_by_lookup_keyval(clusterConfigList, "entity_id", entityId, "kafka_home")
            javaHome          = system_test_utils.get_data_by_lookup_keyval(clusterConfigList, "entity_id", entityId, "java_home")
            jmxPort           = system_test_utils.get_data_by_lookup_keyval(clusterConfigList, "entity_id", entityId, "jmx_port")
            kafkaRunClassBin  = kafkaHome + "/bin/kafka-run-class.sh"

            logger.info("starting kafka migration tool", extra=d)
            migrationToolLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "migration_tool", entityId, "default")
            migrationToolLogPathName = migrationToolLogPath + "/migration_tool.log"
            testcaseEnv.userDefinedEnvVarDict["migrationToolLogPathName"] = migrationToolLogPathName

            testcaseConfigsList = testcaseEnv.testcaseConfigsList
            numProducers    = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "num.producers")
            numStreams      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "num.streams")
            producerConfig  = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "producer.config")
            consumerConfig  = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "consumer.config")
            zkClientJar     = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "zkclient.01.jar")
            kafka07Jar      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "kafka.07.jar")
            whiteList       = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "whitelist")
            logFile         = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "log_filename")

            cmdList = ["ssh " + host,
                       "'JAVA_HOME=" + javaHome,
                       "JMX_PORT=" + jmxPort,
                       kafkaRunClassBin + " kafka.tools.KafkaMigrationTool",
                       "--whitelist="        + whiteList,
                       "--num.producers="    + numProducers,
                       "--num.streams="      + numStreams,
                       "--producer.config="  + systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + producerConfig,
                       "--consumer.config="  + systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + consumerConfig,
                       "--zkclient.01.jar="  + systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + zkClientJar,
                       "--kafka.07.jar="     + systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + kafka07Jar,
                       " &> " + migrationToolLogPath + "/migrationTool.log",
                       " & echo pid:$! > " + migrationToolLogPath + "/entity_" + entityId + "_pid'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)
            system_test_utils.async_sys_call(cmdStr)
            time.sleep(5)

            pidCmdStr = "ssh " + host + " 'cat " + migrationToolLogPath + "/entity_" + entityId + "_pid' 2> /dev/null"
            logger.debug("executing command: [" + pidCmdStr + "]", extra=d)
            subproc = system_test_utils.sys_call_return_subproc(pidCmdStr)

            # keep track of the remote entity pid in a dictionary
            for line in subproc.stdout.readlines():
                if line.startswith("pid"):
                    line = line.rstrip('\n')
                    logger.debug("found pid line: [" + line + "]", extra=d)
                    tokens = line.split(':')
                    testcaseEnv.entityMigrationToolParentPidDict[entityId] = tokens[1]


def validate_07_08_migrated_data_matched(systemTestEnv, testcaseEnv):
    logger.debug("#### Inside validate_07_08_migrated_data_matched", extra=d)

    validationStatusDict        = testcaseEnv.validationStatusDict
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")
    consumerCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "console_consumer")

    for prodPerfCfg in prodPerfCfgList:
        producerEntityId = prodPerfCfg["entity_id"]
        topic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", producerEntityId, "topic")

        consumerEntityIdList = system_test_utils.get_data_from_list_of_dicts(
                               clusterEntityConfigDictList, "role", "console_consumer", "entity_id")

        matchingConsumerEntityId = None
        for consumerEntityId in consumerEntityIdList:
            consumerTopic = system_test_utils.get_data_by_lookup_keyval(
                            testcaseEnv.testcaseConfigsList, "entity_id", consumerEntityId, "topic")
            if consumerTopic in topic:
                matchingConsumerEntityId = consumerEntityId
                break

        if matchingConsumerEntityId is None:
            break

        msgChecksumMissingInConsumerLogPathName = get_testcase_config_log_dir_pathname(
                                                  testcaseEnv, "console_consumer", matchingConsumerEntityId, "default") \
                                                  + "/msg_checksum_missing_in_consumer.log"
        producerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", producerEntityId, "default")
        producerLogPathName = producerLogPath + "/producer_performance.log"

        consumerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", matchingConsumerEntityId, "default")
        consumerLogPathName = consumerLogPath + "/console_consumer.log"

        producerMsgChecksumList      = get_message_checksum(producerLogPathName)
        consumerMsgChecksumList      = get_message_checksum(consumerLogPathName)
        producerMsgChecksumSet       = set(producerMsgChecksumList)
        consumerMsgChecksumSet       = set(consumerMsgChecksumList)
        producerMsgChecksumUniqList  = list(producerMsgChecksumSet)
        consumerMsgChecksumUniqList  = list(consumerMsgChecksumSet)

        missingMsgChecksumInConsumer = producerMsgChecksumSet - consumerMsgChecksumSet

        logger.debug("size of producerMsgChecksumList      : " + str(len(producerMsgChecksumList)), extra=d)
        logger.debug("size of consumerMsgChecksumList      : " + str(len(consumerMsgChecksumList)), extra=d)
        logger.debug("size of producerMsgChecksumSet       : " + str(len(producerMsgChecksumSet)), extra=d)
        logger.debug("size of consumerMsgChecksumSet       : " + str(len(consumerMsgChecksumSet)), extra=d)
        logger.debug("size of producerMsgChecksumUniqList  : " + str(len(producerMsgChecksumUniqList)), extra=d)
        logger.debug("size of consumerMsgChecksumUniqList  : " + str(len(consumerMsgChecksumUniqList)), extra=d)
        logger.debug("size of missingMsgChecksumInConsumer : " + str(len(missingMsgChecksumInConsumer)), extra=d)

        outfile = open(msgChecksumMissingInConsumerLogPathName, "w")
        for id in missingMsgChecksumInConsumer:
            outfile.write(id + "\n")
        outfile.close()

        logger.info("no. of messages on topic [" + topic + "] sent from producer          : " + str(len(producerMsgChecksumList)), extra=d)
        logger.info("no. of messages on topic [" + topic + "] received by consumer        : " + str(len(consumerMsgChecksumList)), extra=d)
        logger.info("no. of unique messages on topic [" + topic + "] sent from producer   : " + str(len(producerMsgChecksumUniqList)),  extra=d)
        logger.info("no. of unique messages on topic [" + topic + "] received by consumer : " + str(len(consumerMsgChecksumUniqList)),  extra=d)
        validationStatusDict["Unique messages from producer on [" + topic + "]"] = str(len(list(producerMsgChecksumSet)))
        validationStatusDict["Unique messages from consumer on [" + topic + "]"] = str(len(list(consumerMsgChecksumSet)))

        if ( len(producerMsgChecksumList) > 0 and len(list(producerMsgChecksumSet)) == len(list(consumerMsgChecksumSet))):
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
        else:
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
            logger.info("See " + msgChecksumMissingInConsumerLogPathName + " for missing MessageID", extra=d)

def validate_broker_log_segment_checksum(systemTestEnv, testcaseEnv, clusterName="source"):
    logger.debug("#### Inside validate_broker_log_segment_checksum", extra=d)

    anonLogger.info("================================================")
    anonLogger.info("validating merged broker log segment checksums")
    anonLogger.info("================================================")

    brokerLogCksumDict   = {}
    testCaseBaseDir      = testcaseEnv.testCaseBaseDir
    tcConfigsList        = testcaseEnv.testcaseConfigsList
    validationStatusDict = testcaseEnv.validationStatusDict
    clusterConfigList    = systemTestEnv.clusterEntityConfigDictList
    #brokerEntityIdList   = system_test_utils.get_data_from_list_of_dicts(clusterConfigList, "role", "broker", "entity_id")
    allBrokerConfigList  = system_test_utils.get_dict_from_list_of_dicts(clusterConfigList, "role", "broker")
    brokerEntityIdList   = system_test_utils.get_data_from_list_of_dicts(allBrokerConfigList, "cluster_name", clusterName, "entity_id")

    # loop through all brokers
    for brokerEntityId in brokerEntityIdList:
        logCksumDict   = {}
        # remoteLogSegmentPathName : /tmp/kafka_server_4_logs
        # => remoteLogSegmentDir   : kafka_server_4_logs
        remoteLogSegmentPathName = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", brokerEntityId, "log.dir")
        remoteLogSegmentDir      = os.path.basename(remoteLogSegmentPathName)
        logPathName              = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")
        localLogSegmentPath      = logPathName + "/" + remoteLogSegmentDir

        # localLogSegmentPath :
        # .../system_test/mirror_maker_testsuite/testcase_5002/logs/broker-4/kafka_server_4_logs
        #   |- test_1-0
        #        |- 00000000000000000000.index
        #        |- 00000000000000000000.log
        #        |- 00000000000000000020.index
        #        |- 00000000000000000020.log
        #        |- . . .
        #   |- test_1-1
        #        |- 00000000000000000000.index
        #        |- 00000000000000000000.log
        #        |- 00000000000000000020.index
        #        |- 00000000000000000020.log
        #        |- . . .

        # loop through all topicPartition directories such as : test_1-0, test_1-1, ...
        for topicPartition in os.listdir(localLogSegmentPath):
            # found a topic-partition directory
            if os.path.isdir(localLogSegmentPath + "/" + topicPartition):
                # md5 hasher
                m = hashlib.md5()

                # logSegmentKey is like this : kafka_server_9_logs:test_1-0 (delimited by ':')
                logSegmentKey = remoteLogSegmentDir + ":" + topicPartition

                # log segment files are located in : localLogSegmentPath + "/" + topicPartition
                # sort the log segment files under each topic-partition and get the md5 checksum
                for logFile in sorted(os.listdir(localLogSegmentPath + "/" + topicPartition)):
                    # only process log file: *.log
                    if logFile.endswith(".log"):
                        # read the log segment file as binary
                        offsetLogSegmentPathName = localLogSegmentPath + "/" + topicPartition + "/" + logFile
                        fin = file(offsetLogSegmentPathName, 'rb')
                        # keep reading 64K max at a time
                        while True:
                            data = fin.read(65536)
                            if not data:
                                fin.close()
                                break
                            # update it into the hasher
                            m.update(data)

                # update the md5 checksum into brokerLogCksumDict with the corresponding key
                brokerLogCksumDict[logSegmentKey] = m.hexdigest()

    # print it out to the console for reference
    pprint.pprint(brokerLogCksumDict)

    # brokerLogCksumDict will look like this:
    # {
    #   'kafka_server_1_logs:tests_1-0': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_1_logs:tests_1-1': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_1_logs:tests_2-0': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_1_logs:tests_2-1': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_2_logs:tests_1-0': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_2_logs:tests_1-1': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_2_logs:tests_2-0': 'd41d8cd98f00b204e9800998ecf8427e',
    #   'kafka_server_2_logs:tests_2-1': 'd41d8cd98f00b204e9800998ecf8427e'
    # }

    checksumDict = {}
    # organize the checksum according to their topic-partition and checksumDict will look like this:
    # {
    #   'test_1-0' : ['d41d8cd98f00b204e9800998ecf8427e','d41d8cd98f00b204e9800998ecf8427e'],
    #   'test_1-1' : ['d41d8cd98f00b204e9800998ecf8427e','d41d8cd98f00b204e9800998ecf8427e'],
    #   'test_2-0' : ['d41d8cd98f00b204e9800998ecf8427e','d41d8cd98f00b204e9800998ecf8427e'],
    #   'test_2-1' : ['d41d8cd98f00b204e9800998ecf8427e','d41d8cd98f00b204e9800998ecf8427e']
    # }

    for brokerTopicPartitionKey, md5Checksum in brokerLogCksumDict.items():
        tokens = brokerTopicPartitionKey.split(":")
        brokerKey      = tokens[0]
        topicPartition = tokens[1]
        if topicPartition in checksumDict:
            # key already exist
            checksumDict[topicPartition].append(md5Checksum)
        else:
            # new key => create a new list to store checksum
            checksumDict[topicPartition] = []
            checksumDict[topicPartition].append(md5Checksum)

    failureCount = 0

    # loop through checksumDict: the checksums should be the same inside each
    # topic-partition's list. Otherwise, checksum mismatched is detected
    for topicPartition, checksumList in checksumDict.items():
        checksumSet = frozenset(checksumList)
        if len(checksumSet) > 1:
            failureCount += 1
            logger.error("merged log segment checksum in " + topicPartition + " mismatched", extra=d)
        elif len(checksumSet) == 1:
            logger.debug("merged log segment checksum in " + topicPartition + " matched", extra=d)
        else:
            logger.error("unexpected error in " + topicPartition, extra=d)

    if failureCount == 0:
        validationStatusDict["Validate for merged log segment checksum in cluster [" + clusterName + "]"] = "PASSED"
    else:
        validationStatusDict["Validate for merged log segment checksum in cluster [" + clusterName + "]"] = "FAILED"

def start_simple_consumer(systemTestEnv, testcaseEnv, minStartingOffsetDict=None):

    clusterList        = systemTestEnv.clusterEntityConfigDictList
    consumerConfigList = system_test_utils.get_dict_from_list_of_dicts(clusterList, "role", "console_consumer")
    for consumerConfig in consumerConfigList:
        host              = consumerConfig["hostname"]
        entityId          = consumerConfig["entity_id"]
        jmxPort           = consumerConfig["jmx_port"]
        clusterName       = consumerConfig["cluster_name"]
        kafkaHome         = system_test_utils.get_data_by_lookup_keyval(clusterList, "entity_id", entityId, "kafka_home")
        javaHome          = system_test_utils.get_data_by_lookup_keyval(clusterList, "entity_id", entityId, "java_home")
        kafkaRunClassBin  = kafkaHome + "/bin/kafka-run-class.sh"
        consumerLogPath   = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", entityId, "default")

        if host != "localhost":
            consumerLogPath = replace_kafka_home(consumerLogPath, kafkaHome)

        # testcase configurations:
        testcaseList = testcaseEnv.testcaseConfigsList
        topic = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "topic")

        brokerListStr = ""
        if clusterName == "source":
            brokerListStr  = testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"]
        elif clusterName == "target":
            brokerListStr  = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]
        else:
            logger.error("Invalid cluster name : " + clusterName, extra=d)
            raise Exception("Invalid cluster name : " + clusterName)

        if len(brokerListStr) == 0:
            logger.error("Empty broker list str", extra=d)
            raise Exception("Empty broker list str")

        numPartitions = None
        try:
            numPartitions = testcaseEnv.testcaseArgumentsDict["num_partition"]
        except:
            pass

        if numPartitions is None:
            logger.error("Invalid no. of partitions: " + numPartitions, extra=d)
            raise Exception("Invalid no. of partitions: " + numPartitions)
        else:
            numPartitions = int(numPartitions)

        replicaIndex   = 1
        startingOffset = -2
        brokerPortList = brokerListStr.split(',')
        for brokerPort in brokerPortList:

            partitionId = 0
            while (partitionId < numPartitions):
                logger.info("starting debug consumer for replica on [" + brokerPort + "] partition [" + str(partitionId) + "]", extra=d)

                if minStartingOffsetDict is not None:
                    topicPartition = topic + "-" + str(partitionId)
                    startingOffset = minStartingOffsetDict[topicPartition]

                outputFilePathName = consumerLogPath + "/simple_consumer_" + topic + "-" + str(partitionId) + "_r" + str(replicaIndex) + ".log"
                brokerPortLabel = brokerPort.replace(":", "_")
                cmdList = ["ssh " + host,
                           "'JAVA_HOME=" + javaHome,
                           kafkaRunClassBin + " kafka.tools.SimpleConsumerShell",
                           "--broker-list " + brokerListStr,
                           "--topic " + topic,
                           "--partition " + str(partitionId),
                           "--replica " + str(replicaIndex),
                           "--offset " + str(startingOffset),
                           "--no-wait-at-logend ",
                           " > " + outputFilePathName,
                           " & echo pid:$! > " + consumerLogPath + "/entity_" + entityId + "_pid'"]

                cmdStr = " ".join(cmdList)

                logger.debug("executing command: [" + cmdStr + "]", extra=d)
                subproc_1 = system_test_utils.sys_call_return_subproc(cmdStr)
                # dummy for-loop to wait until the process is completed
                for line in subproc_1.stdout.readlines():
                    pass
                time.sleep(1)

                partitionId += 1
            replicaIndex += 1

def get_controller_attributes(systemTestEnv, testcaseEnv):

    logger.info("Querying Zookeeper for Controller info ...", extra=d)

    # keep track of controller data in this dict such as broker id & entity id
    controllerDict = {}

    clusterConfigsList = systemTestEnv.clusterEntityConfigDictList
    tcConfigsList      = testcaseEnv.testcaseConfigsList

    zkDictList         = system_test_utils.get_dict_from_list_of_dicts(clusterConfigsList, "role", "zookeeper")
    firstZkDict        = zkDictList[0]
    hostname           = firstZkDict["hostname"]
    zkEntityId         = firstZkDict["entity_id"]
    clientPort         = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", zkEntityId, "clientPort")
    kafkaHome          = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "kafka_home")
    javaHome           = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "java_home")
    kafkaRunClassBin   = kafkaHome + "/bin/kafka-run-class.sh"

    cmdStrList = ["ssh " + hostname,
                  "\"JAVA_HOME=" + javaHome,
                  kafkaRunClassBin + " kafka.tools.ZooKeeperMainWrapper ",
                  "-server " + testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"],
                  "get /controller 2> /dev/null | tail -1\""]

    cmdStr = " ".join(cmdStrList)
    logger.debug("executing command [" + cmdStr + "]", extra=d)
    subproc = system_test_utils.sys_call_return_subproc(cmdStr)
    for line in subproc.stdout.readlines():
        if "brokerid" in line:
            json_str  = line.rstrip('\n')
            json_data = json.loads(json_str)
            brokerid  = str(json_data["brokerid"])
            controllerDict["brokerid"]  = brokerid
            controllerDict["entity_id"] = system_test_utils.get_data_by_lookup_keyval(
                                              tcConfigsList, "broker.id", brokerid, "entity_id")
        else:
            pass

    return controllerDict

def getMinCommonStartingOffset(systemTestEnv, testcaseEnv, clusterName="source"):

    brokerLogStartOffsetDict = {}
    minCommonStartOffsetDict = {}

    tcConfigsList        = testcaseEnv.testcaseConfigsList
    clusterConfigList    = systemTestEnv.clusterEntityConfigDictList
    allBrokerConfigList  = system_test_utils.get_dict_from_list_of_dicts(clusterConfigList, "role", "broker")
    brokerEntityIdList   = system_test_utils.get_data_from_list_of_dicts(allBrokerConfigList, "cluster_name", clusterName, "entity_id")

    # loop through all brokers
    for brokerEntityId in sorted(brokerEntityIdList):
        # remoteLogSegmentPathName : /tmp/kafka_server_4_logs
        # => remoteLogSegmentDir   : kafka_server_4_logs
        remoteLogSegmentPathName = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", brokerEntityId, "log.dir")
        remoteLogSegmentDir      = os.path.basename(remoteLogSegmentPathName)
        logPathName              = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")
        localLogSegmentPath      = logPathName + "/" + remoteLogSegmentDir

        # loop through all topicPartition directories such as : test_1-0, test_1-1, ...
        for topicPartition in sorted(os.listdir(localLogSegmentPath)):
            # found a topic-partition directory
            if os.path.isdir(localLogSegmentPath + "/" + topicPartition):

                # startingOffsetKey : <brokerEntityId>:<topicPartition>  (eg. 1:test_1-0)
                startingOffsetKey = brokerEntityId + ":" + topicPartition

                # log segment files are located in : localLogSegmentPath + "/" + topicPartition
                # sort the log segment files under each topic-partition
                for logFile in sorted(os.listdir(localLogSegmentPath + "/" + topicPartition)):

                    # logFile is located at:
                    # system_test/xxxx_testsuite/testcase_xxxx/logs/broker-1/kafka_server_1_logs/test_1-0/00000000000000003800.log
                    if logFile.endswith(".log"):
                        matchObj = re.match("0*(.*)\.log", logFile)    # remove the leading zeros & the file extension
                        startingOffset = matchObj.group(1)             # this is the starting offset from the file name
                        if len(startingOffset) == 0:                   # when log filename is: 00000000000000000000.log
                            startingOffset = "0"

                        # starting offset of a topic-partition can be retrieved from the filename of the first log segment
                        # => break out of this innest for-loop after processing the first log segment file
                        brokerLogStartOffsetDict[startingOffsetKey] = startingOffset
                        break

    # brokerLogStartOffsetDict is like this:
    # {u'1:test_1-0': u'400',
    #  u'1:test_1-1': u'400',
    #  u'1:test_2-0': u'200',
    #  u'1:test_2-1': u'200',
    #  u'2:test_1-0': u'400',
    #  u'2:test_1-1': u'400',
    #  u'2:test_2-0': u'200',
    #  u'2:test_2-1': u'200',
    #  u'3:test_1-0': '0',
    #  u'3:test_1-1': '0',
    #  u'3:test_2-0': '0',
    #  u'3:test_2-1': '0'}

    # loop through brokerLogStartOffsetDict to get the min common starting offset for each topic-partition
    for brokerTopicPartition in sorted(brokerLogStartOffsetDict.iterkeys()):
        topicPartition = brokerTopicPartition.split(':')[1]

        if topicPartition in minCommonStartOffsetDict:
            # key exists => if the new value is greater, replace the existing value with new
            if minCommonStartOffsetDict[topicPartition] < brokerLogStartOffsetDict[brokerTopicPartition]:
                minCommonStartOffsetDict[topicPartition] = brokerLogStartOffsetDict[brokerTopicPartition]
        else:
            # key doesn't exist => add it to the dictionary
            minCommonStartOffsetDict[topicPartition] = brokerLogStartOffsetDict[brokerTopicPartition]

    # returning minCommonStartOffsetDict which is like this:
    # {u'test_1-0': u'400',
    #  u'test_1-1': u'400',
    #  u'test_2-0': u'200',
    #  u'test_2-1': u'200'}
    return minCommonStartOffsetDict

def validate_simple_consumer_data_matched_across_replicas(systemTestEnv, testcaseEnv):
    logger.debug("#### Inside validate_simple_consumer_data_matched_across_replicas", extra=d)

    validationStatusDict        = testcaseEnv.validationStatusDict
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    consumerEntityIdList        = system_test_utils.get_data_from_list_of_dicts(
                                  clusterEntityConfigDictList, "role", "console_consumer", "entity_id")
    replicaFactor               = testcaseEnv.testcaseArgumentsDict["replica_factor"]
    numPartition                = testcaseEnv.testcaseArgumentsDict["num_partition"]

    for consumerEntityId in consumerEntityIdList:

        # get topic string from multi consumer "entity"
        topicStr  = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", consumerEntityId, "topic")

        # the topic string could be multi topics separated by ','
        topicList = topicStr.split(',')

        for topic in topicList:
            logger.debug("working on topic : " + topic, extra=d)
            consumerLogPath = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", consumerEntityId, "default")

            # keep track of total msg count across replicas for each topic-partition
            # (should be greater than 0 for passing)
            totalMsgCounter = 0

            # keep track of the mismatch msg count for each topic-partition
            # (should be equal to 0 for passing)
            mismatchCounter = 0

            replicaIdxMsgIdList = []
            # replicaIdxMsgIdList :
            # - This is a list of dictionaries of topic-partition (key)
            #   mapping to list of MessageID in that topic-partition (val)
            # - The list index is mapped to (replicaId - 1)
            # [
            #  // list index = 0 => replicaId = idx(0) + 1 = 1
            #  {
            #      "topic1-0" : [ "0000000001", "0000000002", "0000000003"],
            #      "topic1-1" : [ "0000000004", "0000000005", "0000000006"]
            #  },
            #  // list index = 1 => replicaId = idx(1) + 1 = 2
            #  {
            #      "topic1-0" : [ "0000000001", "0000000002", "0000000003"],
            #      "topic1-1" : [ "0000000004", "0000000005", "0000000006"]
            #  }
            # ]

            # initialize replicaIdxMsgIdList
            j = 0
            while j < int(replicaFactor):
                newDict = {}
                replicaIdxMsgIdList.append(newDict)
                j += 1

            # retrieve MessageID from all simple consumer log4j files
            for logFile in sorted(os.listdir(consumerLogPath)):

                if logFile.startswith("simple_consumer_"+topic) and logFile.endswith(".log"):
                    logger.debug("working on file : " + logFile, extra=d)
                    matchObj    = re.match("simple_consumer_"+topic+"-(\d*)_r(\d*)\.log" , logFile)
                    partitionId = int(matchObj.group(1))
                    replicaIdx  = int(matchObj.group(2))

                    consumerLogPathName   = consumerLogPath + "/" + logFile
                    consumerMsgIdList     = get_message_id(consumerLogPathName)

                    topicPartition = topic + "-" + str(partitionId)
                    replicaIdxMsgIdList[replicaIdx - 1][topicPartition] = consumerMsgIdList

                    logger.info("no. of messages on topic [" + topic + "] at " + logFile + " : " + str(len(consumerMsgIdList)), extra=d)
                    validationStatusDict["No. of messages from consumer on [" + topic + "] at " + logFile] = str(len(consumerMsgIdList))

            # print replicaIdxMsgIdList

            # take the first dictionary of replicaIdxMsgIdList and compare with the rest
            firstMsgIdDict = replicaIdxMsgIdList[0]

            # loop through all 'topic-partition' such as topic1-0, topic1-1, ...
            for topicPartition in sorted(firstMsgIdDict.iterkeys()):

                # compare all replicas' MessageID in corresponding topic-partition
                for i in range(len(replicaIdxMsgIdList)):
                    # skip the first dictionary
                    if i == 0:
                        totalMsgCounter += len(firstMsgIdDict[topicPartition])
                        continue

                    totalMsgCounter += len(replicaIdxMsgIdList[i][topicPartition])

                    # get the count of mismatch MessageID between first MessageID list and the other lists
                    diffCount = system_test_utils.diff_lists(firstMsgIdDict[topicPartition], replicaIdxMsgIdList[i][topicPartition])
                    mismatchCounter += diffCount
                    logger.info("Mismatch count of topic-partition [" + topicPartition + "] in replica id [" + str(i+1) + "] : " + str(diffCount), extra=d)

            if mismatchCounter == 0 and totalMsgCounter > 0:
                validationStatusDict["Validate for data matched on topic [" + topic + "] across replicas"] = "PASSED"
            else:
                validationStatusDict["Validate for data matched on topic [" + topic + "] across replicas"] = "FAILED"


def validate_data_matched_in_multi_topics_from_single_consumer_producer(systemTestEnv, testcaseEnv, replicationUtils):
    logger.debug("#### Inside validate_data_matched_in_multi_topics_from_single_consumer_producer", extra=d)

    validationStatusDict        = testcaseEnv.validationStatusDict
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")

    for prodPerfCfg in prodPerfCfgList:
        producerEntityId = prodPerfCfg["entity_id"]
        topicStr = testcaseEnv.producerTopicsString
        acks     = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", producerEntityId, "request-num-acks")

        consumerEntityIdList = system_test_utils.get_data_from_list_of_dicts(clusterEntityConfigDictList, "role", "console_consumer", "entity_id")

        matchingConsumerEntityId = None
        for consumerEntityId in consumerEntityIdList:
            consumerTopic = testcaseEnv.consumerTopicsString
            if consumerTopic in topicStr:
                matchingConsumerEntityId = consumerEntityId
                break

        if matchingConsumerEntityId is None:
            break

        producerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", producerEntityId, "default")
        producerLogPathName = producerLogPath + "/producer_performance.log"

        consumerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", matchingConsumerEntityId, "default")
        consumerLogPathName = consumerLogPath + "/console_consumer.log"

        topicList = topicStr.split(',')
        for topic in topicList:
            consumerDuplicateCount = 0
            msgIdMissingInConsumerLogPathName = get_testcase_config_log_dir_pathname(
                                                testcaseEnv, "console_consumer", matchingConsumerEntityId, "default") \
                                                + "/msg_id_missing_in_consumer_" + topic + ".log"
            producerMsgIdList  = get_message_id(producerLogPathName, topic)
            consumerMsgIdList  = get_message_id(consumerLogPathName, topic)
            producerMsgIdSet   = set(producerMsgIdList)
            consumerMsgIdSet   = set(consumerMsgIdList)

            consumerDuplicateCount = len(consumerMsgIdList) -len(consumerMsgIdSet)
            missingUniqConsumerMsgId = system_test_utils.subtract_list(producerMsgIdSet, consumerMsgIdSet)

            outfile = open(msgIdMissingInConsumerLogPathName, "w")
            for id in missingUniqConsumerMsgId:
                outfile.write(id + "\n")
            outfile.close()

            logger.info("Producer entity id " + producerEntityId, extra=d)
            logger.info("Consumer entity id " + matchingConsumerEntityId, extra=d)
            logger.info("no. of unique messages on topic [" + topic + "] sent from publisher  : " + str(len(producerMsgIdSet)), extra=d)
            logger.info("no. of unique messages on topic [" + topic + "] received by consumer : " + str(len(consumerMsgIdSet)), extra=d)
            logger.info("no. of duplicate messages on topic [" + topic + "] received by consumer: " + str(consumerDuplicateCount), extra=d)
            validationStatusDict["Unique messages from producer on [" + topic + "]"] = str(len(producerMsgIdSet))
            validationStatusDict["Unique messages from consumer on [" + topic + "]"] = str(len(consumerMsgIdSet))

            missingPercentage = len(missingUniqConsumerMsgId) * 100.00 / len(producerMsgIdSet)
            logger.info("Data loss threshold % : " + str(replicationUtils.ackOneDataLossThresholdPercent), extra=d)
            logger.warn("Data loss % on topic : " + topic + " : " + str(missingPercentage), extra=d)

            if ( len(missingUniqConsumerMsgId) == 0 and len(producerMsgIdSet) > 0 ):
                validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
            elif (acks == "1"):
                if missingPercentage <= replicationUtils.ackOneDataLossThresholdPercent:
                    validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
                    logger.warn("Test case (Acks = 1) passes with less than " + str(replicationUtils.ackOneDataLossThresholdPercent) \
                        + "% data loss : [" + str(len(missingUniqConsumerMsgId)) + "] missing messages", extra=d)
                else:
                    validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
                    logger.error("Test case (Acks = 1) failed with more than " + str(replicationUtils.ackOneDataLossThresholdPercent) \
                        + "% data loss : [" + str(len(missingUniqConsumerMsgId)) + "] missing messages", extra=d)
            else:
                validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
                logger.info("See " + msgIdMissingInConsumerLogPathName + " for missing MessageID", extra=d)


def validate_index_log(systemTestEnv, testcaseEnv, clusterName="source"):
    logger.debug("#### Inside validate_index_log", extra=d)

    failureCount         = 0
    brokerLogCksumDict   = {}
    testCaseBaseDir      = testcaseEnv.testCaseBaseDir
    tcConfigsList        = testcaseEnv.testcaseConfigsList
    validationStatusDict = testcaseEnv.validationStatusDict
    clusterConfigList    = systemTestEnv.clusterEntityConfigDictList
    allBrokerConfigList  = system_test_utils.get_dict_from_list_of_dicts(clusterConfigList, "role", "broker")
    brokerEntityIdList   = system_test_utils.get_data_from_list_of_dicts(allBrokerConfigList, "cluster_name", clusterName, "entity_id")

    # loop through all brokers
    for brokerEntityId in brokerEntityIdList:
        logCksumDict   = {}
        # remoteLogSegmentPathName : /tmp/kafka_server_4_logs
        # => remoteLogSegmentDir   : kafka_server_4_logs
        remoteLogSegmentPathName = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", brokerEntityId, "log.dir")
        remoteLogSegmentDir      = os.path.basename(remoteLogSegmentPathName)
        logPathName              = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")
        localLogSegmentPath      = logPathName + "/" + remoteLogSegmentDir
        kafkaHome                = system_test_utils.get_data_by_lookup_keyval(clusterConfigList, "entity_id", brokerEntityId, "kafka_home")
        hostname                 = system_test_utils.get_data_by_lookup_keyval(clusterConfigList, "entity_id", brokerEntityId, "hostname")
        kafkaRunClassBin         = kafkaHome + "/bin/kafka-run-class.sh"

        # localLogSegmentPath :
        # .../system_test/mirror_maker_testsuite/testcase_5002/logs/broker-4/kafka_server_4_logs
        #   |- test_1-0
        #        |- 00000000000000000000.index
        #        |- 00000000000000000000.log
        #        |- 00000000000000000020.index
        #        |- 00000000000000000020.log
        #        |- . . .
        #   |- test_1-1
        #        |- 00000000000000000000.index
        #        |- 00000000000000000000.log
        #        |- 00000000000000000020.index
        #        |- 00000000000000000020.log
        #        |- . . .

        # loop through all topicPartition directories such as : test_1-0, test_1-1, ...
        for topicPartition in os.listdir(localLogSegmentPath):
            # found a topic-partition directory
            if os.path.isdir(localLogSegmentPath + "/" + topicPartition):

                # log segment files are located in : localLogSegmentPath + "/" + topicPartition
                # sort the log segment files under each topic-partition and verify index
                for logFile in sorted(os.listdir(localLogSegmentPath + "/" + topicPartition)):
                    # only process index file: *.index
                    if logFile.endswith(".index"):
                        offsetLogSegmentPathName = localLogSegmentPath + "/" + topicPartition + "/" + logFile
                        cmdStrList = ["ssh " + hostname,
                                      kafkaRunClassBin + " kafka.tools.DumpLogSegments",
                                      " --file " + offsetLogSegmentPathName,
                                      "--verify-index-only 2>&1"]
                        cmdStr     = " ".join(cmdStrList)

                        showMismatchedIndexOffset = False

                        logger.debug("executing command [" + cmdStr + "]", extra=d)
                        subproc = system_test_utils.sys_call_return_subproc(cmdStr)
                        for line in subproc.stdout.readlines():
                            line = line.rstrip('\n')
                            if showMismatchedIndexOffset:
                                logger.debug("#### [" + line + "]", extra=d)
                            elif "Mismatches in :" in line:
                                logger.debug("#### error found [" + line + "]", extra=d)
                                failureCount += 1
                                showMismatchedIndexOffset = True

    if failureCount == 0:
        validationStatusDict["Validate index log in cluster [" + clusterName + "]"] = "PASSED"
    else:
        validationStatusDict["Validate index log in cluster [" + clusterName + "]"] = "FAILED"

def get_leader_for(systemTestEnv, testcaseEnv, topic, partition):
    logger.info("Querying Zookeeper for leader info for topic " + topic, extra=d)
    clusterConfigsList = systemTestEnv.clusterEntityConfigDictList
    tcConfigsList      = testcaseEnv.testcaseConfigsList

    zkDictList         = system_test_utils.get_dict_from_list_of_dicts(clusterConfigsList, "role", "zookeeper")
    firstZkDict        = zkDictList[0]
    hostname           = firstZkDict["hostname"]
    zkEntityId         = firstZkDict["entity_id"]
    clientPort         = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", zkEntityId, "clientPort")
    kafkaHome          = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "kafka_home")
    javaHome           = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "java_home")
    kafkaRunClassBin   = kafkaHome + "/bin/kafka-run-class.sh"

    zkQueryStr = "get /brokers/topics/" + topic + "/partitions/" + str(partition) + "/state"
    brokerid   = ''
    leaderEntityId = ''

    cmdStrList = ["ssh " + hostname,
                  "\"JAVA_HOME=" + javaHome,
                  kafkaRunClassBin + " kafka.tools.ZooKeeperMainWrapper ",
                  "-server " + testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"],
                  zkQueryStr + " 2> /dev/null | tail -1\""]
    cmdStr = " ".join(cmdStrList)
    logger.info("executing command [" + cmdStr + "]", extra=d)
    subproc = system_test_utils.sys_call_return_subproc(cmdStr)
    for line in subproc.stdout.readlines():
        if "\"leader\"" in line:
            line = line.rstrip('\n')
            json_data = json.loads(line)
            for key,val in json_data.items():
                if key == 'leader':
                    brokerid = str(val)
            leaderEntityId = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "broker.id", brokerid, "entity_id")
            break
    return leaderEntityId

def get_leader_attributes(systemTestEnv, testcaseEnv):

    logger.info("Querying Zookeeper for leader info ...", extra=d)

    # keep track of leader data in this dict such as broker id & entity id
    leaderDict = {}

    clusterConfigsList = systemTestEnv.clusterEntityConfigDictList
    tcConfigsList      = testcaseEnv.testcaseConfigsList

    zkDictList         = system_test_utils.get_dict_from_list_of_dicts(clusterConfigsList, "role", "zookeeper")
    firstZkDict        = zkDictList[0]
    hostname           = firstZkDict["hostname"]
    zkEntityId         = firstZkDict["entity_id"]
    clientPort         = system_test_utils.get_data_by_lookup_keyval(tcConfigsList, "entity_id", zkEntityId, "clientPort")
    kafkaHome          = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "kafka_home")
    javaHome           = system_test_utils.get_data_by_lookup_keyval(clusterConfigsList, "entity_id", zkEntityId, "java_home")
    kafkaRunClassBin   = kafkaHome + "/bin/kafka-run-class.sh"

    # this should have been updated in start_producer_in_thread
    producerTopicsString = testcaseEnv.producerTopicsString
    topics = producerTopicsString.split(',')
    zkQueryStr = "get /brokers/topics/" + topics[0] + "/partitions/0/state"
    brokerid   = ''

    cmdStrList = ["ssh " + hostname,
                  "\"JAVA_HOME=" + javaHome,
                  kafkaRunClassBin + " kafka.tools.ZooKeeperMainWrapper ",
                  "-server " + testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"],
                  zkQueryStr + " 2> /dev/null | tail -1\""]
    cmdStr = " ".join(cmdStrList)
    logger.info("executing command [" + cmdStr + "]", extra=d)

    subproc = system_test_utils.sys_call_return_subproc(cmdStr)
    for line in subproc.stdout.readlines():
        if "\"leader\"" in line:
            line = line.rstrip('\n')
            json_data = json.loads(line)
            for key,val in json_data.items():
                if key == 'leader':
                    brokerid = str(val)

            leaderDict["brokerid"]  = brokerid
            leaderDict["topic"]     = topics[0]
            leaderDict["partition"] = '0'
            leaderDict["entity_id"] = system_test_utils.get_data_by_lookup_keyval(
                                          tcConfigsList, "broker.id", brokerid, "entity_id")
            leaderDict["hostname"]  = system_test_utils.get_data_by_lookup_keyval(
                                          clusterConfigsList, "entity_id", leaderDict["entity_id"], "hostname")
            break

    print leaderDict
    return leaderDict

def write_consumer_properties(consumerProperties):
    import tempfile
    props_file_path = tempfile.gettempdir() + "/consumer.properties"
    consumer_props_file=open(props_file_path,"w")
    for key,value in consumerProperties.iteritems():
        consumer_props_file.write(key+"="+value+"\n")
    consumer_props_file.close()
    return props_file_path

