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
# system_test_utils.py
# ===================================

import copy
import difflib
import inspect
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import time

logger  = logging.getLogger("namedLogger")
aLogger = logging.getLogger("anonymousLogger")
thisClassName = '(system_test_utils)'
d = {'name_of_class': thisClassName}


def get_current_unix_timestamp():
    ts = time.time()
    return "{0:.6f}".format(ts)


def get_local_hostname():
    return socket.gethostname()


def sys_call(cmdStr):
    output = ""
    #logger.info("executing command [" + cmdStr + "]", extra=d)
    p = subprocess.Popen(cmdStr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
        output += line
    return output


def remote_async_sys_call(host, cmd):
    cmdStr = "ssh " + host + " \"" + cmd + "\""
    logger.info("executing command [" + cmdStr + "]", extra=d)
    async_sys_call(cmdStr)


def remote_sys_call(host, cmd):
    cmdStr = "ssh " + host + " \"" + cmd + "\""
    logger.info("executing command [" + cmdStr + "]", extra=d)
    sys_call(cmdStr)


def get_dir_paths_with_prefix(fullPath, dirNamePrefix):
    dirsList = []
    for dirName in os.listdir(fullPath):
        if not os.path.isfile(dirName) and dirName.startswith(dirNamePrefix):
            dirsList.append(os.path.abspath(fullPath + "/" + dirName))
    return dirsList


def get_testcase_prop_json_pathname(testcasePathName):
    testcaseDirName = os.path.basename(testcasePathName)
    return testcasePathName + "/" + testcaseDirName + "_properties.json"

def get_json_list_data(infile):
    json_file_str = open(infile, "r").read()
    json_data     = json.loads(json_file_str)
    data_list     = []

    for key,settings in json_data.items():
        if type(settings) == list:
            for setting in settings:
                if type(setting) == dict:
                    kv_dict = {}
                    for k,v in setting.items():
                        kv_dict[k] = v
                    data_list.append(kv_dict)

    return data_list


def get_dict_from_list_of_dicts(listOfDicts, lookupKey, lookupVal):
    # {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '0', 'role': 'zookeeper', 'hostname': 'localhost'}
    # {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '1', 'role': 'broker', 'hostname': 'localhost'}
    #
    # Usage:
    #
    # 1. get_data_from_list_of_dicts(self.clusterConfigsList, "entity_id", "0", "role")
    #    returns:
    #        {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '0', 'role': 'zookeeper', 'hostname': 'localhost'}
    #
    # 2. get_data_from_list_of_dicts(self.clusterConfigsList, None, None, "role")
    #    returns:
    #        {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '0', 'role': 'zookeeper', 'hostname': 'localhost'}
    #        {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '1', 'role': 'broker', 'hostname': 'localhost'}

    retList = []
    if ( lookupVal is None or lookupKey is None ):
        for dict in listOfDicts:
            for k,v in dict.items():
                if ( k == fieldToRetrieve ):               # match with fieldToRetrieve ONLY
                    retList.append( dict )
    else:
        for dict in listOfDicts:
            for k,v in dict.items():
                if ( k == lookupKey and v == lookupVal ):  # match with lookupKey and lookupVal
                    retList.append( dict )

    return retList


def get_data_from_list_of_dicts(listOfDicts, lookupKey, lookupVal, fieldToRetrieve):
    # Sample List of Dicts:
    # {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '0', 'role': 'zookeeper', 'hostname': 'localhost'}
    # {'kafka_home': '/mnt/u001/kafka_0.8_sanity', 'entity_id': '1', 'role': 'broker', 'hostname': 'localhost'}
    #
    # Usage:
    # 1. get_data_from_list_of_dicts(self.clusterConfigsList, "entity_id", "0", "role")
    #    => returns ['zookeeper']
    # 2. get_data_from_list_of_dicts(self.clusterConfigsList, None, None, "role")
    #    => returns ['zookeeper', 'broker']

    retList = []
    if ( lookupVal is None or lookupKey is None ):
        for dict in listOfDicts:
            for k,v in dict.items():
                if ( k == fieldToRetrieve ):               # match with fieldToRetrieve ONLY
                    try:
                        retList.append( dict[fieldToRetrieve] )
                    except:
                        logger.debug("field not found: " + fieldToRetrieve, extra=d)
    else:
        for dict in listOfDicts:
            for k,v in dict.items():
                if ( k == lookupKey and v == lookupVal ):  # match with lookupKey and lookupVal
                    try:
                        retList.append( dict[fieldToRetrieve] )
                    except:
                        logger.debug("field not found: " + fieldToRetrieve, extra=d)
    return retList

def get_data_by_lookup_keyval(listOfDict, lookupKey, lookupVal, fieldToRetrieve):
    returnValue = ""
    returnValuesList = get_data_from_list_of_dicts(listOfDict, lookupKey, lookupVal, fieldToRetrieve)
    if len(returnValuesList) > 0:
        returnValue = returnValuesList[0]

    return returnValue

def get_json_dict_data(infile):
    json_file_str = open(infile, "r").read()
    json_data     = json.loads(json_file_str)
    data_dict     = {}

    for key,val in json_data.items():
        if ( type(val) != list ): 
            data_dict[key] = val

    return data_dict

def get_remote_child_processes(hostname, pid):
    pidStack = []

    cmdList = ['''ssh ''' + hostname,
              ''''pid=''' + pid + '''; prev_pid=""; echo $pid;''',
              '''while [[ "x$pid" != "x" ]];''',
              '''do prev_pid=$pid;''',
              '''  for child in $(ps -o pid,ppid ax | awk "{ if ( \$2 == $pid ) { print \$1 }}");''',
              '''    do echo $child; pid=$child;''',
              '''  done;''',
              '''  if [ $prev_pid == $pid ]; then''',
              '''    break;''',
              '''  fi;''',
              '''done' 2> /dev/null''']

    cmdStr = " ".join(cmdList)
    logger.debug("executing command [" + cmdStr, extra=d)

    subproc = subprocess.Popen(cmdStr, shell=True, stdout=subprocess.PIPE)
    for line in subproc.stdout.readlines():
        procId = line.rstrip('\n')
        pidStack.append(procId)
    return pidStack

def get_child_processes(pid):
    pidStack   = []
    currentPid = pid
    parentPid  = ""
    pidStack.append(pid)

    while ( len(currentPid) > 0 ):
        psCommand = subprocess.Popen("ps -o pid --ppid %s --noheaders" % currentPid, shell=True, stdout=subprocess.PIPE)
        psOutput  = psCommand.stdout.read()
        outputLine = psOutput.rstrip('\n')
        childPid   = outputLine.lstrip()

        if ( len(childPid) > 0 ):
            pidStack.append(childPid)
            currentPid = childPid
        else:
            break
    return pidStack

def sigterm_remote_process(hostname, pidStack):

    while ( len(pidStack) > 0 ):
        pid = pidStack.pop()
        cmdStr = "ssh " + hostname + " 'kill -15 " + pid + "'"

        try:
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            sys_call_return_subproc(cmdStr)
        except:
            print "WARN - pid:",pid,"not found"
            raise

def sigkill_remote_process(hostname, pidStack):

    while ( len(pidStack) > 0 ):
        pid = pidStack.pop()
        cmdStr = "ssh " + hostname + " 'kill -9 " + pid + "'"

        try:
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            sys_call_return_subproc(cmdStr)
        except:
            print "WARN - pid:",pid,"not found"
            raise

def simulate_garbage_collection_pause_in_remote_process(hostname, pidStack, pauseTimeInSeconds):
    pausedPidStack = []

    # pause the processes
    while len(pidStack) > 0:
        pid = pidStack.pop()
        pausedPidStack.append(pid)
        cmdStr = "ssh " + hostname + " 'kill -SIGSTOP " + pid + "'"

        try:
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            sys_call_return_subproc(cmdStr)
        except:
            print "WARN - pid:",pid,"not found"
            raise

    time.sleep(int(pauseTimeInSeconds))

    # resume execution of the processes
    while len(pausedPidStack) > 0:
        pid = pausedPidStack.pop()
        cmdStr = "ssh " + hostname + " 'kill -SIGCONT " + pid + "'"

        try:
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            sys_call_return_subproc(cmdStr)
        except:
            print "WARN - pid:",pid,"not found"
            raise

def terminate_process(pidStack):
    while ( len(pidStack) > 0 ):
        pid = pidStack.pop()
        try:
            os.kill(int(pid), signal.SIGTERM)
        except:
            print "WARN - pid:",pid,"not found"
            raise


def convert_keyval_to_cmd_args(configFilePathname):
    cmdArg  = ""
    inlines = open(configFilePathname, "r").readlines()
    for inline in inlines:
        line = inline.rstrip()
        tokens = line.split('=', 1)

        if (len(tokens) == 2):
            cmdArg = cmdArg + " --" + tokens[0] + " " + tokens[1]
        elif (len(tokens) == 1):
            cmdArg = cmdArg + " --" + tokens[0]
        else:
            print "ERROR: unexpected arguments list", line
    return cmdArg


def async_sys_call(cmd_str):
    subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def sys_call_return_subproc(cmd_str):
    p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p


def remote_host_file_exists(hostname, pathname):
    cmdStr = "ssh " + hostname + " 'ls " + pathname + "'"
    logger.debug("executing command: [" + cmdStr + "]", extra=d)
    subproc = sys_call_return_subproc(cmdStr)

    for line in subproc.stdout.readlines():
        if "No such file or directory" in line:
            return False
    return True


def remote_host_directory_exists(hostname, path):
    cmdStr = "ssh " + hostname + " 'ls -d " + path + "'"
    logger.debug("executing command: [" + cmdStr + "]", extra=d)
    subproc = sys_call_return_subproc(cmdStr)

    for line in subproc.stdout.readlines():
        if "No such file or directory" in line:
            return False
    return True


def remote_host_processes_stopped(hostname):
    cmdStr = "ssh " + hostname + \
             " \"ps auxw | grep -v grep | grep -v Bootstrap | grep -i 'java\|run\-\|producer\|consumer\|jmxtool\|kafka' | wc -l\" 2> /dev/null"

    logger.info("executing command: [" + cmdStr + "]", extra=d)
    subproc = sys_call_return_subproc(cmdStr)

    for line in subproc.stdout.readlines():
        line = line.rstrip('\n')
        logger.info("no. of running processes found : [" + line + "]", extra=d)
        if line == '0':
            return True
    return False


def setup_remote_hosts(systemTestEnv):
    # sanity check on remote hosts to make sure:
    # - all directories (eg. java_home) specified in cluster_config.json exists in all hosts
    # - no conflicting running processes in remote hosts

    aLogger.info("=================================================")
    aLogger.info("setting up remote hosts ...")
    aLogger.info("=================================================")

    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    localKafkaHome = os.path.abspath(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/..")

    # when configuring "default" java_home, use JAVA_HOME environment variable, if exists
    # otherwise, use the directory with the java binary
    localJavaHome  = os.environ.get('JAVA_HOME')
    if localJavaHome is not None:
        localJavaBin   = localJavaHome + '/bin/java'
    else:
        subproc = sys_call_return_subproc("which java")
        for line in subproc.stdout.readlines():
            if line.startswith("which: no "):
                logger.error("No Java binary found in local host", extra=d)
                return False
            else:
                line = line.rstrip('\n')
                localJavaBin = line
                matchObj = re.match("(.*)\/bin\/java$", line)
                localJavaHome = matchObj.group(1)

    listIndex = -1
    for clusterEntityConfigDict in clusterEntityConfigDictList:
        listIndex += 1

        hostname  = clusterEntityConfigDict["hostname"]
        kafkaHome = clusterEntityConfigDict["kafka_home"]
        javaHome  = clusterEntityConfigDict["java_home"]

        if hostname == "localhost" and javaHome == "default":
            clusterEntityConfigDictList[listIndex]["java_home"] = localJavaHome

        if hostname == "localhost" and kafkaHome == "default":
            clusterEntityConfigDictList[listIndex]["kafka_home"] = localKafkaHome
        if hostname == "localhost" and kafkaHome == "system_test/migration_tool_testsuite/0.7":
            clusterEntityConfigDictList[listIndex]["kafka_home"] = localKafkaHome + "/system_test/migration_tool_testsuite/0.7"

        kafkaHome = clusterEntityConfigDict["kafka_home"]
        javaHome  = clusterEntityConfigDict["java_home"]

        logger.debug("checking java binary [" + localJavaBin + "] in host [" + hostname + "]", extra=d)
        if not remote_host_directory_exists(hostname, javaHome):
            logger.error("Directory not found: [" + javaHome + "] in host [" + hostname + "]", extra=d)
            return False

        logger.debug("checking directory [" + kafkaHome + "] in host [" + hostname + "]", extra=d)
        if not remote_host_directory_exists(hostname, kafkaHome):
            logger.info("Directory not found: [" + kafkaHome + "] in host [" + hostname + "]", extra=d)
            if hostname == "localhost":
                return False
            else:
                localKafkaSourcePath = systemTestEnv.SYSTEM_TEST_BASE_DIR + "/.."
                logger.debug("copying local copy of [" + localKafkaSourcePath + "] to " + hostname + ":" + kafkaHome, extra=d)
                copy_source_to_remote_hosts(hostname, localKafkaSourcePath, kafkaHome)

    return True

def copy_source_to_remote_hosts(hostname, sourceDir, destDir):

    cmdStr = "rsync -avz --delete-before " + sourceDir + "/ " + hostname + ":" + destDir
    logger.info("executing command [" + cmdStr + "]", extra=d)
    subproc = sys_call_return_subproc(cmdStr)

    for line in subproc.stdout.readlines():
        dummyVar = 1


def remove_kafka_home_dir_at_remote_hosts(hostname, kafkaHome):

    if remote_host_file_exists(hostname, kafkaHome + "/bin/kafka-run-class.sh"):
        cmdStr  = "ssh " + hostname + " 'chmod -R 777 " + kafkaHome + "'"
        logger.info("executing command [" + cmdStr + "]", extra=d)
        sys_call(cmdStr)

        cmdStr  = "ssh " + hostname + " 'rm -rf " + kafkaHome + "'"
        logger.info("executing command [" + cmdStr + "]", extra=d)
        #sys_call(cmdStr)
    else:
        logger.warn("possible destructive command [" + cmdStr + "]", extra=d)
        logger.warn("check config file: system_test/cluster_config.properties", extra=d)
        logger.warn("aborting test...", extra=d)
        sys.exit(1)

def get_md5_for_file(filePathName, blockSize=8192):
    md5 = hashlib.md5()
    f   = open(filePathName, 'rb')

    while True:
        data = f.read(blockSize)
        if not data:
            break
        md5.update(data)
    return md5.digest()

def load_cluster_config(clusterConfigPathName, clusterEntityConfigDictList):
    # empty the list
    clusterEntityConfigDictList[:] = []

    # retrieve each entity's data from cluster config json file
    # as "dict" and enter them into a "list"
    jsonFileContent = open(clusterConfigPathName, "r").read()
    jsonData        = json.loads(jsonFileContent)
    for key, cfgList in jsonData.items():
        if key == "cluster_config":
            for cfg in cfgList:
                clusterEntityConfigDictList.append(cfg)

def setup_remote_hosts_with_testcase_level_cluster_config(systemTestEnv, testCasePathName):
    # =======================================================================
    # starting a new testcase, check for local cluster_config.json
    # =======================================================================
    # 1. if there is a xxxx_testsuite/testcase_xxxx/cluster_config.json
    #    => load it into systemTestEnv.clusterEntityConfigDictList
    # 2. if there is NO testcase_xxxx/cluster_config.json but has a xxxx_testsuite/cluster_config.json
    #    => retore systemTestEnv.clusterEntityConfigDictListLastFoundInTestSuite
    # 3. if there is NO testcase_xxxx/cluster_config.json NOR xxxx_testsuite/cluster_config.json
    #    => restore system_test/cluster_config.json

    testCaseLevelClusterConfigPathName = testCasePathName + "/cluster_config.json"

    if os.path.isfile(testCaseLevelClusterConfigPathName):
        # if there is a cluster_config.json in this directory, load it and use it for this testsuite
        logger.info("found a new cluster_config : " + testCaseLevelClusterConfigPathName, extra=d)

        # empty the current cluster config list
        systemTestEnv.clusterEntityConfigDictList[:] = []

        # load the cluster config for this testcase level
        load_cluster_config(testCaseLevelClusterConfigPathName, systemTestEnv.clusterEntityConfigDictList)

        # back up this testcase level cluster config
        systemTestEnv.clusterEntityConfigDictListLastFoundInTestCase = copy.deepcopy(systemTestEnv.clusterEntityConfigDictList)

    elif len(systemTestEnv.clusterEntityConfigDictListLastFoundInTestSuite) > 0:
        # if there is NO testcase_xxxx/cluster_config.json, but has a xxxx_testsuite/cluster_config.json
        # => restore the config in xxxx_testsuite/cluster_config.json

        # empty the current cluster config list
        systemTestEnv.clusterEntityConfigDictList[:] = []

        # restore the system_test/cluster_config.json
        systemTestEnv.clusterEntityConfigDictList = copy.deepcopy(systemTestEnv.clusterEntityConfigDictListLastFoundInTestSuite)

    else:
        # if there is NONE, restore the config in system_test/cluster_config.json

        # empty the current cluster config list
        systemTestEnv.clusterEntityConfigDictList[:] = []

        # restore the system_test/cluster_config.json
        systemTestEnv.clusterEntityConfigDictList = copy.deepcopy(systemTestEnv.clusterEntityConfigDictListInSystemTestLevel)

    # set up remote hosts
    if not setup_remote_hosts(systemTestEnv):
        logger.error("Remote hosts sanity check failed. Aborting test ...", extra=d)
        print
        sys.exit(1)
    print

def setup_remote_hosts_with_testsuite_level_cluster_config(systemTestEnv, testModulePathName):
    # =======================================================================
    # starting a new testsuite, check for local cluster_config.json:
    # =======================================================================
    # 1. if there is a xxxx_testsuite/cluster_config.son
    #    => load it into systemTestEnv.clusterEntityConfigDictList
    # 2. if there is NO xxxx_testsuite/cluster_config.son
    #    => restore system_test/cluster_config.json

    testSuiteLevelClusterConfigPathName = testModulePathName + "/cluster_config.json"

    if os.path.isfile(testSuiteLevelClusterConfigPathName):
        # if there is a cluster_config.json in this directory, load it and use it for this testsuite
        logger.info("found a new cluster_config : " + testSuiteLevelClusterConfigPathName, extra=d)

        # empty the current cluster config list
        systemTestEnv.clusterEntityConfigDictList[:] = []

        # load the cluster config for this testsuite level
        load_cluster_config(testSuiteLevelClusterConfigPathName, systemTestEnv.clusterEntityConfigDictList)

        # back up this testsuite level cluster config
        systemTestEnv.clusterEntityConfigDictListLastFoundInTestSuite = copy.deepcopy(systemTestEnv.clusterEntityConfigDictList)

    else:
        # if there is NONE, restore the config in system_test/cluster_config.json

        # empty the last testsuite level cluster config list
        systemTestEnv.clusterEntityConfigDictListLastFoundInTestSuite[:] = []

        # empty the current cluster config list
        systemTestEnv.clusterEntityConfigDictList[:] = []

        # restore the system_test/cluster_config.json
        systemTestEnv.clusterEntityConfigDictList = copy.deepcopy(systemTestEnv.clusterEntityConfigDictListInSystemTestLevel)

    # set up remote hosts
    if not setup_remote_hosts(systemTestEnv):
        logger.error("Remote hosts sanity check failed. Aborting test ...", extra=d)
        print
        sys.exit(1)
    print

# =================================================
# lists_diff_count
# - find the no. of different items in both lists
# - both lists need not be sorted
# - input lists won't be changed
# =================================================
def lists_diff_count(a, b):
    c = list(b)
    d = []
    for item in a:
        try:
            c.remove(item)
        except:
            d.append(item)

    if len(d) > 0:
        print "#### Mismatch MessageID"
        print d

    return len(c) + len(d)

# =================================================
# subtract_list
# - subtract items in listToSubtract from mainList
#   and return the resulting list
# - both lists need not be sorted
# - input lists won't be changed
# =================================================
def subtract_list(mainList, listToSubtract):
    remainingList = list(mainList)
    for item in listToSubtract:
        try:
            remainingList.remove(item)
        except:
            pass
    return remainingList

# =================================================
# diff_lists
# - find the diff of 2 lists and return the 
#   total no. of mismatch from both lists
# - diff of both lists includes:
#   - no. of items mismatch
#   - ordering of the items
#
# sample lists:
# a = ['8','4','3','2','1']
# b = ['8','3','4','2','1']
#
# difflib will return the following:
#   8
# + 3
#   4
# - 3
#   2
#   1
#
# diff_lists(a,b) returns 2 and prints the following:
# #### only in seq 2 :  + 3
# #### only in seq 1 :  - 3
# =================================================
def diff_lists(a, b):
    mismatchCount = 0
    d = difflib.Differ()
    diff = d.compare(a,b)

    for item in diff:
        result = item[0:1].strip()
        if len(result) > 0:
            mismatchCount += 1
            if '-' in result:
                logger.debug("#### only in seq 1 : " + item, extra=d)
            elif '+' in result:
                logger.debug("#### only in seq 2 : " + item, extra=d)

    return mismatchCount

