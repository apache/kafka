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

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:
        hostname   = clusterEntityConfigDict["hostname"]
        entity_id  = clusterEntityConfigDict["entity_id"]
        role       = clusterEntityConfigDict["role"]

        logger.debug("entity_id : " + entity_id, extra=d)
        logger.debug("hostname  : " + hostname,  extra=d)
        logger.debug("role      : " + role,      extra=d)

        configPathName     = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "config")
        metricsPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "metrics")
        logPathName        = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "default")

        # ==============================
        # collect entity log file
        # ==============================
        cmdList = ["scp",
                   hostname + ":" + logPathName + "/*",
                   logPathName]
        cmdStr  = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)

        # ==============================
        # collect entity metrics file
        # ==============================
        cmdList = ["scp",
                   hostname + ":" + metricsPathName + "/*",
                   metricsPathName]
        cmdStr  = " ".join(cmdList)
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        system_test_utils.sys_call(cmdStr)

    # ==============================
    # collect dashboards file
    # ==============================
    dashboardsPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "dashboards")
    cmdList = ["scp",
               hostname + ":" + dashboardsPathName + "/*",
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

        logger.debug("entity_id : " + entity_id, extra=d)
        logger.debug("hostname  : " + hostname, extra=d)
        logger.debug("role      : " + role, extra=d)

        configPathName     = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "config")
        metricsPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "metrics")
        dashboardsPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entity_id, "dashboards")

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
    # to construct a zk.connect str for broker in the form of:
    # zk.connect=<host1>:<port1>,<host2>:<port2>,...
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
            logger.error("Unknown cluster name: " + clusterName)
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
            logger.error("Unknown cluster name: " + clusterName)
            sys.exit(1)

    # for each entity in the cluster config
    for clusterCfg in clusterConfigsList:
        cl_entity_id = clusterCfg["entity_id"]

        # loop through testcase config list 'tcConfigsList' for a matching cluster entity_id
        for tcCfg in tcConfigsList:
            if (tcCfg["entity_id"] == cl_entity_id):

                # copy the associated .properties template, update values, write to testcase_<xxx>/config

                if ( clusterCfg["role"] == "broker" ):
                    if clusterCfg["cluster_name"] == "source":
                        tcCfg["zk.connect"] = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
                    elif clusterCfg["cluster_name"] == "target":
                        tcCfg["zk.connect"] = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
                    else:
                        logger.error("Unknown cluster name: " + clusterName)
                        sys.exit(1)

                    copy_file_with_dict_values(cfgTemplatePathname + "/server.properties",
                        cfgDestPathname + "/" + tcCfg["config_filename"], tcCfg, None)

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
                        logger.error("Unknown cluster name: " + clusterName)
                        sys.exit(1)

                elif ( clusterCfg["role"] == "mirror_maker"):
                    tcCfg["broker.list"] = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]
                    copy_file_with_dict_values(cfgTemplatePathname + "/mirror_producer.properties",
                        cfgDestPathname + "/" + tcCfg["mirror_producer_config_filename"], tcCfg, None)

                    # update zk.connect with the zk entities specified in cluster_config.json
                    tcCfg["zk.connect"] = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
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
        testcasePathName = testcaseEnv.testCaseBaseDir

        cmdStr = "scp " + testcasePathName + "/config/* " + hostname + ":" + testcasePathName + "/config"
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


def start_mirror_makers(systemTestEnv, testcaseEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    brokerEntityIdList = system_test_utils.get_data_from_list_of_dicts( 
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
        logFile    = system_test_utils.get_data_by_lookup_keyval( \
                         testcaseEnv.testcaseConfigsList, "entity_id", brokerEntityId, "log_filename")

        logPathName = get_testcase_config_log_dir_pathname(testcaseEnv, "broker", brokerEntityId, "default")
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

    mmConsumerConfigFile = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId,
                           "mirror_consumer_config_filename")
    mmProducerConfigFile = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId,
                           "mirror_producer_config_filename")

    logger.info("starting " + role + " in host [" + hostname + "] on client port [" + clientPort + "]", extra=d)

    configPathName = get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "config")
    logPathName    = get_testcase_config_log_dir_pathname(testcaseEnv, role, entityId, "default")

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
                  kafkaHome + "/bin/kafka-run-class.sh kafka.Kafka",
                  configPathName + "/" + configFile + " >> ",
                  logPathName + "/" + logFile + " & echo pid:$! > ",
                  logPathName + "/entity_" + entityId + "_pid'"]

    elif role == "mirror_maker":
        cmdList = ["ssh " + hostname,
                  "'JAVA_HOME=" + javaHome,
                 "JMX_PORT=" + jmxPort,
                  kafkaHome + "/bin/kafka-run-class.sh kafka.tools.MirrorMaker",
                  "--consumer.config " + configPathName + "/" + mmConsumerConfigFile,
                  "--producer.config " + configPathName + "/" + mmProducerConfigFile,
                  "--whitelist=\".*\" >> ",
                  logPathName + "/" + logFile + " & echo pid:$! > ",
                  logPathName + "/entity_" + entityId + "_pid'"]

    cmdStr = " ".join(cmdList)

    logger.debug("executing command: [" + cmdStr + "]", extra=d)
    system_test_utils.async_sys_call(cmdStr)
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

    time.sleep(1)
    if role != "mirror_maker":
        metrics.start_metrics_collection(hostname, jmxPort, role, entityId, systemTestEnv, testcaseEnv)


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

        consumerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "console_consumer", entityId, "default")
        consumerLogPathName = consumerLogPath + "/console_consumer.log"

        testcaseEnv.userDefinedEnvVarDict["consumerLogPathName"] = consumerLogPathName

        # testcase configurations:
        testcaseList = testcaseEnv.testcaseConfigsList
        topic     = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "topic")
        timeoutMs = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "consumer-timeout-ms")


        formatterOption = ""
        try:
            formatterOption = system_test_utils.get_data_by_lookup_keyval(testcaseList, "entity_id", entityId, "formatter")
        except:
            pass

        if len(formatterOption) > 0:
            formatterOption = " --formatter " + formatterOption + " "

        zkConnectStr = ""
        if clusterName == "source":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]
        elif clusterName == "target":
            zkConnectStr = testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]
        else:
            logger.error("Invalid cluster name : " + clusterName)
            sys.exit(1)

        cmdList = ["ssh " + host,
                   "'JAVA_HOME=" + javaHome,
                   "JMX_PORT=" + jmxPort,
                   kafkaRunClassBin + " kafka.consumer.ConsoleConsumer",
                   "--zookeeper " + zkConnectStr,
                   "--topic " + topic,
                   "--consumer-timeout-ms " + timeoutMs,
                   formatterOption,
                   "--from-beginning ",
                   " >> " + consumerLogPathName,
                   " & echo pid:$! > " + consumerLogPath + "/entity_" + entityId + "_pid'"]

        cmdStr = " ".join(cmdList)

        logger.debug("executing command: [" + cmdStr + "]", extra=d)
        system_test_utils.async_sys_call(cmdStr)
        time.sleep(2)
        metrics.start_metrics_collection(host, jmxPort, role, entityId, systemTestEnv, testcaseEnv)

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
        testcaseEnv.lock.acquire()
        testcaseEnv.numProducerThreadsRunning += 1
        logger.debug("testcaseEnv.numProducerThreadsRunning : " + str(testcaseEnv.numProducerThreadsRunning), extra=d)
        time.sleep(1)
        testcaseEnv.lock.release()
        time.sleep(1)
        metrics.start_metrics_collection(host, jmxPort, role, entityId, systemTestEnv, testcaseEnv)

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

    # testcase configurations:
    testcaseConfigsList = testcaseEnv.testcaseConfigsList
    topic          = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "topic")
    threads        = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "threads")
    compCodec      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "compression-codec")
    messageSize    = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "message-size")
    noMsgPerBatch  = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "message")
    requestNumAcks = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "request-num-acks")
    asyncMode      = system_test_utils.get_data_by_lookup_keyval(testcaseConfigsList, "entity_id", entityId, "async")

    brokerListStr  = ""
    if clusterName == "source":
        brokerListStr  = testcaseEnv.userDefinedEnvVarDict["sourceBrokerList"]
    elif clusterName == "target":
        brokerListStr  = testcaseEnv.userDefinedEnvVarDict["targetBrokerList"]
    else:
        logger.error("Unknown cluster name: " + clusterName)
        sys.exit(1)

    logger.info("starting producer preformance", extra=d)

    producerLogPath     = get_testcase_config_log_dir_pathname(testcaseEnv, "producer_performance", entityId, "default")
    producerLogPathName = producerLogPath + "/producer_performance.log"

    testcaseEnv.userDefinedEnvVarDict["producerLogPathName"] = producerLogPathName

    counter = 0
    producerSleepSec = int(testcaseEnv.testcaseArgumentsDict["sleep_seconds_between_producer_calls"])

    boolArgumentsStr = ""
    if asyncMode.lower() == "true":
        boolArgumentsStr = boolArgumentsStr + " --async"

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
                       kafkaRunClassBin + " kafka.perf.ProducerPerformance",
                       "--broker-list " + brokerListStr,
                       "--initial-message-id " + str(initMsgId),
                       "--messages " + noMsgPerBatch,
                       "--topic " + topic,
                       "--threads " + threads,
                       "--compression-codec " + compCodec,
                       "--message-size " + messageSize,
                       "--request-num-acks " + requestNumAcks,
                       boolArgumentsStr,
                       " >> " + producerLogPathName,
                       " & echo pid:$! > " + producerLogPath + "/entity_" + entityId + "_pid'"]

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
                       kafkaRunClassBin + " kafka.perf.ProducerPerformance",
                       "--brokerinfo " + brokerInfoStr,
                       "--messages " + noMsgPerBatch,
                       "--topic " + topic,
                       "--threads " + threads,
                       "--compression-codec " + compCodec,
                       "--message-size " + messageSize,
                       "--vary-message-size --async",
                       " >> " + producerLogPathName,
                       " & echo pid:$! > " + producerLogPath + "/entity_" + entityId + "_pid'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)

            subproc = system_test_utils.sys_call_return_subproc(cmdStr)
            for line in subproc.stdout.readlines():
                pass    # dummy loop to wait until producer is completed
        else:
            testcaseEnv.numProducerThreadsRunning -= 1
            logger.debug("testcaseEnv.numProducerThreadsRunning : " + str(testcaseEnv.numProducerThreadsRunning), extra=d)
            testcaseEnv.lock.release()
            break

        counter += 1
        logger.debug("calling testcaseEnv.lock.release()", extra=d)
        testcaseEnv.lock.release()
        time.sleep(int(producerSleepSec))

    # wait until other producer threads also stops and
    # let the main testcase know all producers have stopped
    while 1:
        testcaseEnv.lock.acquire()
        time.sleep(1)
        if testcaseEnv.numProducerThreadsRunning == 0:
            testcaseEnv.userDefinedEnvVarDict["backgroundProducerStopped"] = True
            testcaseEnv.lock.release()
            break
        else:
            logger.debug("waiting for TRUE of testcaseEnv.userDefinedEnvVarDict['backgroundProducerStopped']", extra=d)
            testcaseEnv.lock.release()
        time.sleep(1)

def stop_remote_entity(systemTestEnv, entityId, parentPid):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    hostname  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "hostname")
    pidStack  = system_test_utils.get_remote_child_processes(hostname, parentPid)

    logger.debug("terminating process id: " + parentPid + " in host: " + hostname, extra=d)
    system_test_utils.sigterm_remote_process(hostname, pidStack)


def force_stop_remote_entity(systemTestEnv, entityId, parentPid):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    hostname  = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", entityId, "hostname")
    pidStack  = system_test_utils.get_remote_child_processes(hostname, parentPid)

    logger.debug("terminating process id: " + parentPid + " in host: " + hostname, extra=d)
    system_test_utils.sigkill_remote_process(hostname, pidStack)


def create_topic(systemTestEnv, testcaseEnv):
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")

    for prodPerfCfg in prodPerfCfgList:
        topic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", prodPerfCfg["entity_id"], "topic")
        zkEntityId      = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "entity_id")
        zkHost          = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "role", "zookeeper", "hostname")
        kafkaHome       = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "kafka_home")
        javaHome        = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", zkEntityId, "java_home")
        createTopicBin  = kafkaHome + "/bin/kafka-create-topic.sh"

        logger.debug("zkEntityId : " + zkEntityId, extra=d)
        logger.debug("createTopicBin : " + createTopicBin, extra=d)

        if len(testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"]) > 0:
            logger.info("creating topic: [" + topic + "] at: [" + testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"] + "]", extra=d) 
            cmdList = ["ssh " + zkHost,
                       "'JAVA_HOME=" + javaHome,
                       createTopicBin,
                       " --topic "     + topic,
                       " --zookeeper " + testcaseEnv.userDefinedEnvVarDict["sourceZkConnectStr"],
                       " --replica "   + testcaseEnv.testcaseArgumentsDict["replica_factor"],
                       " --partition " + testcaseEnv.testcaseArgumentsDict["num_partition"] + " >> ",
                       testcaseEnv.testCaseBaseDir + "/logs/create_source_cluster_topic.log'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)
            subproc = system_test_utils.sys_call_return_subproc(cmdStr)

        if len(testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"]) > 0:
            logger.info("creating topic: [" + topic + "] at: [" + testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"] + "]", extra=d) 
            cmdList = ["ssh " + zkHost,
                       "'JAVA_HOME=" + javaHome,
                       createTopicBin,
                       " --topic "     + topic,
                       " --zookeeper " + testcaseEnv.userDefinedEnvVarDict["targetZkConnectStr"],
                       " --replica "   + testcaseEnv.testcaseArgumentsDict["replica_factor"],
                       " --partition " + testcaseEnv.testcaseArgumentsDict["num_partition"] + " >> ",
                       testcaseEnv.testCaseBaseDir + "/logs/create_target_cluster_topic.log'"]

            cmdStr = " ".join(cmdList)
            logger.debug("executing command: [" + cmdStr + "]", extra=d)
            subproc = system_test_utils.sys_call_return_subproc(cmdStr)

def get_message_id(logPathName):
    logLines      = open(logPathName, "r").readlines()
    messageIdList = []

    for line in logLines:
        if not "MessageID" in line:
            continue
        else:
            matchObj = re.match('.*MessageID:(.*?):', line)
            messageIdList.append( matchObj.group(1) )

    return messageIdList

def get_message_checksum(logPathName):
    logLines = open(logPathName, "r").readlines()
    messageChecksumList = []

    for line in logLines:
        if not "checksum:" in line:
            continue
        else:
            matchObj = re.match('.*checksum:(\d*?).*', line)
            if matchObj is not None:
                messageChecksumList.append( matchObj.group(1) )
            else:
                logger.error("unexpected log line : " + line, extra=d)

    return messageChecksumList


def validate_data_matched(systemTestEnv, testcaseEnv):
    validationStatusDict        = testcaseEnv.validationStatusDict
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    prodPerfCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "producer_performance")
    consumerCfgList = system_test_utils.get_dict_from_list_of_dicts(clusterEntityConfigDictList, "role", "console_consumer")

    for prodPerfCfg in prodPerfCfgList:
        producerEntityId = prodPerfCfg["entity_id"]
        topic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", producerEntityId, "topic")

        consumerEntityIdList = system_test_utils.get_data_from_list_of_dicts( \
                           clusterEntityConfigDictList, "role", "console_consumer", "entity_id")

        matchingConsumerEntityId = None
        for consumerEntityId in consumerEntityIdList:
            consumerTopic = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", consumerEntityId, "topic")
            if consumerTopic in topic:
                matchingConsumerEntityId = consumerEntityId
                break

        if matchingConsumerEntityId is None:
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

        missingMsgIdInConsumer = producerMsgIdSet - consumerMsgIdSet

        outfile = open(msgIdMissingInConsumerLogPathName, "w")
        for id in missingMsgIdInConsumer:
            outfile.write(id + "\n")
        outfile.close()

        logger.info("no. of unique messages on topic [" + topic + "] sent from publisher  : " + str(len(producerMsgIdSet)), extra=d)
        logger.info("no. of unique messages on topic [" + topic + "] received by consumer : " + str(len(consumerMsgIdSet)), extra=d)
        validationStatusDict["Unique messages from producer on [" + topic + "]"] = str(len(producerMsgIdSet))
        validationStatusDict["Unique messages from consumer on [" + topic + "]"] = str(len(consumerMsgIdSet))

        if ( len(missingMsgIdInConsumer) == 0 and len(producerMsgIdSet) > 0 ):
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
            #return True
        else:
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
            logger.info("See " + msgIdMissingInConsumerLogPathName + " for missing MessageID", extra=d)
            #return False


def validate_leader_election_successful(testcaseEnv, leaderDict, validationStatusDict):

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

    for clusterEntityConfigDict in systemTestEnv.clusterEntityConfigDictList:

        hostname         = clusterEntityConfigDict["hostname"]
        entityId         = clusterEntityConfigDict["entity_id"]
        role             = clusterEntityConfigDict["role"]
        kafkaHome        = clusterEntityConfigDict["kafka_home"]
        testCaseBaseDir  = testcaseEnv.testCaseBaseDir
        cmdStr           = ""
        dataDir          = ""

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
            cmdStr = "ssh " + hostname + " \"find " + testCaseBaseDir + " -name '*.log' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + testCaseBaseDir + " -name '*_pid' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + testCaseBaseDir + " -name '*.csv' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + testCaseBaseDir + " -name '*.svg' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

            cmdStr = "ssh " + hostname + " \"find " + testCaseBaseDir + " -name '*.html' | xargs rm 2> /dev/null\""
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            system_test_utils.sys_call(cmdStr)

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
        stop_remote_entity(systemTestEnv, leaderEntityId, leaderPPid)

    logger.info("sleeping for 10s for leader re-election to complete", extra=d)
    time.sleep(10)

    # get broker shut down completed timestamp
    shutdownBrokerDict = get_broker_shutdown_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict)
    logger.debug("unix timestamp of shut down completed: " + str("{0:.6f}".format(shutdownBrokerDict["timestamp"])), extra=d)

    logger.info("looking up new leader", extra=d)

    leaderDict2 = get_leader_elected_log_line(systemTestEnv, testcaseEnv, leaderAttributesDict)
    logger.debug("unix timestamp of new elected leader: " + str("{0:.6f}".format(leaderDict2["timestamp"])), extra=d)

    leaderReElectionLatency = float(leaderDict2["timestamp"]) - float(shutdownBrokerDict["timestamp"])
    logger.info("leader Re-election Latency: " + str(leaderReElectionLatency) + " sec", extra=d)
 
    return leaderReElectionLatency

def validate_broker_log_segment_checksum(systemTestEnv, testcaseEnv):

    anonLogger.info("================================================")
    anonLogger.info("validating broker log segment checksums")
    anonLogger.info("================================================")

    # brokerLogCksumDict -
    #   a dictionary to keep track of log segment files in each brokers and will look like this:
    #
    # {u'broker-1': {'test_1-0/00000000000000000000.kafka': '91500855',
    #                'test_1-0/00000000000000010255.kafka': '1906285795',
    #                'test_1-1/00000000000000000000.kafka': '3785861722',
    #                'test_1-1/00000000000000010322.kafka': '1731628308'},
    #  u'broker-2': {'test_1-0/00000000000000000000.kafka': '91500855',
    #                'test_1-0/00000000000000010255.kafka': '1906285795',
    #                'test_1-1/00000000000000000000.kafka': '3785861722',
    #                'test_1-1/00000000000000010322.kafka': '1731628308'},
    #  u'broker-3': {'test_1-0/00000000000000000000.kafka': '91500855',
    #                'test_1-0/00000000000000010255.kafka': '1906285795',
    #                'test_1-1/00000000000000000000.kafka': '3431356313'}}
    brokerLogCksumDict = {}

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList
    brokerEntityIdList = system_test_utils.get_data_from_list_of_dicts(clusterEntityConfigDictList, "role", "broker", "entity_id")

    # access all brokers' hosts to get broker id and its corresponding log
    # segment file checksums and populate brokerLogCksumDict
    for brokerEntityId in brokerEntityIdList:
        logCksumDict       = {}

        hostname = system_test_utils.get_data_by_lookup_keyval(clusterEntityConfigDictList, "entity_id", brokerEntityId, "hostname")
        logDir   = system_test_utils.get_data_by_lookup_keyval(testcaseEnv.testcaseConfigsList, "entity_id", brokerEntityId, "log.dir")

        # get the log segment file full path name
        cmdStr   = "ssh " + hostname + " \"find " + logDir + " -name '*.log'\" 2> /dev/null"
        logger.debug("executing command [" + cmdStr + "]", extra=d)
        subproc  = system_test_utils.sys_call_return_subproc(cmdStr)
        for line in subproc.stdout.readlines():
            # Need a key to identify each corresponding log segment file in different brokers:
            #   This can be achieved by using part of the full log segment file path as a key to identify
            #   the individual log segment file checksums. The key can be extracted from the path name
            #   and starting from "topic-partition" such as:
            #     full log segment path name   : /tmp/kafka_server_1_logs/test_1-0/00000000000000010255.kafka
            #     part of the path name as key : test_1-0/00000000000000010255.kafka
            logSegmentPathName = line.rstrip('\n')
            substrIndex        = logSegmentPathName.index(logDir) + len(logDir + "/")
            logSegmentFile     = logSegmentPathName[substrIndex:]

            # get log segment file checksum
            cksumCmdStr = "ssh " + hostname + " \"cksum " + logSegmentPathName + " | cut -f1 -d ' '\" 2> /dev/null"
            subproc2 = system_test_utils.sys_call_return_subproc(cksumCmdStr)
            for line2 in subproc2.stdout.readlines():
                checksum = line2.rstrip('\n')
                # use logSegmentFile as a key to associate with its checksum
                logCksumDict[logSegmentFile] = checksum

        # associate this logCksumDict with its broker id
        brokerLogCksumDict["broker-"+brokerEntityId] = logCksumDict

    # use a list of sets for checksums comparison
    sets = []
    for brokerId, logCksumDict in brokerLogCksumDict.items():
        sets.append( set(logCksumDict.items()) )

    # looping through the sets and compare between broker[n] & broker[n+1] ...
    idx = 0
    diffItemSet = None
    while idx < len(sets) - 1:
        diffItemSet = sets[idx] ^ sets[idx + 1]

        if (len(diffItemSet) > 0):
            logger.error("Mismatch found : " + str(diffItemSet), extra=d)
            testcaseEnv.validationStatusDict["Log segment checksum matching across all replicas"] = "FAILED"

            # get the mismatched items key, i.e. the log segment file name
            diffItemList = list(diffItemSet)
            diffItemKeys = []
            for keyvalSet in diffItemList:
                keyvalList = list(keyvalSet)
                diffItemKeys.append(keyvalList[0])

            # mismatch found - so print out the whole log segment file checksum
            # info with the mismatched checksum highlighted
            for brokerId in sorted(brokerLogCksumDict.iterkeys()):
                logCksumDict = brokerLogCksumDict[brokerId]
                print brokerId,":"
                for logSegmentFile in sorted(logCksumDict.iterkeys()):
                    checksum = logCksumDict[logSegmentFile]
                    sys.stdout.write(logSegmentFile + " => " + checksum)
                    try:
                        if diffItemKeys.index(logSegmentFile) >= 0:
                            sys.stdout.write("    <<<< not matching across all replicas")
                    except:
                        pass
                    print
                print
            return
        idx += 1

    # getting here means all log segment checksums matched
    testcaseEnv.validationStatusDict["Log segment checksum matching across all replicas"] = "PASSED"

    anonLogger.info("log segment files checksum :")
    print
    pprint.pprint(brokerLogCksumDict)
    print

def stop_all_remote_running_processes(systemTestEnv, testcaseEnv):

    entityConfigs = systemTestEnv.clusterEntityConfigDictList

    for hostname, producerPPid in testcaseEnv.producerHostParentPidDict.items():
        producerEntityId = system_test_utils.get_data_by_lookup_keyval(entityConfigs, "hostname", hostname, "entity_id")
        stop_remote_entity(systemTestEnv, producerEntityId, producerPPid)

    for hostname, consumerPPid in testcaseEnv.consumerHostParentPidDict.items():
        consumerEntityId = system_test_utils.get_data_by_lookup_keyval(entityConfigs, "hostname", hostname, "entity_id")
        stop_remote_entity(systemTestEnv, consumerEntityId, consumerPPid)

    for entityId, jmxParentPidList in testcaseEnv.entityJmxParentPidDict.items():
        for jmxParentPid in jmxParentPidList:
            stop_remote_entity(systemTestEnv, entityId, jmxParentPid)

    for entityId, mirrorMakerParentPid in testcaseEnv.entityMirrorMakerParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, mirrorMakerParentPid)

    for entityId, brokerParentPid in testcaseEnv.entityBrokerParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, brokerParentPid)

    for entityId, zkParentPid in testcaseEnv.entityZkParentPidDict.items():
        stop_remote_entity(systemTestEnv, entityId, zkParentPid)


def start_migration_tool(systemTestEnv, testcaseEnv):
    clusterConfigList = systemTestEnv.clusterEntityConfigDictList
    migrationToolConfigList = system_test_utils.get_dict_from_list_of_dicts(clusterConfigList, "role", "migration_tool")

    migrationToolConfig = migrationToolConfigList[0]
    host              = migrationToolConfig["hostname"]
    entityId          = migrationToolConfig["entity_id"]
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

def validate_07_08_migrated_data_matched(systemTestEnv, testcaseEnv):
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

        producerMsgChecksumList   = get_message_checksum(producerLogPathName)
        consumerMsgChecksumList   = get_message_checksum(consumerLogPathName)
        producerMsgChecksumSet    = set(producerMsgChecksumList)
        consumerMsgChecksumSet    = set(consumerMsgChecksumList)

        missingMsgChecksumInConsumer = producerMsgChecksumSet - consumerMsgChecksumSet

        outfile = open(msgChecksumMissingInConsumerLogPathName, "w")
        for id in missingMsgChecksumInConsumer:
            outfile.write(id + "\n")
        outfile.close()

        logger.info("no. of unique messages on topic [" + topic + "] sent from publisher  : " + str(len(producerMsgChecksumList)), extra=d)
        logger.info("no. of unique messages on topic [" + topic + "] received by consumer : " + str(len(consumerMsgChecksumList)), extra=d)
        validationStatusDict["Unique messages from producer on [" + topic + "]"] = str(len(producerMsgChecksumList))
        validationStatusDict["Unique messages from consumer on [" + topic + "]"] = str(len(consumerMsgChecksumList))

        if ( len(missingMsgChecksumInConsumer) == 0 and len(producerMsgChecksumList) > 0 ):
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "PASSED"
            #return True
        else:
            validationStatusDict["Validate for data matched on topic [" + topic + "]"] = "FAILED"
            logger.info("See " + msgChecksumMissingInConsumerLogPathName + " for missing MessageID", extra=d)
            #return False


