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

import inspect
import json
import logging
import os
import signal
import subprocess
import sys
import time

logger = logging.getLogger("namedLogger")
thisClassName = '(system_test_utils)'
d = {'name_of_class': thisClassName}


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


def sigkill_remote_process(hostname, pidStack):

    while ( len(pidStack) > 0 ):
        pid = pidStack.pop()
        cmdStr = "ssh " + hostname + " 'kill -9 " + pid + "'"

        try:
            logger.debug("executing command [" + cmdStr + "]", extra=d)
            sys_call_return_subproc(cmdStr)
        except:
            print "WARN - pid:",pid,"not found"


def terminate_process(pidStack):
    while ( len(pidStack) > 0 ):
        pid = pidStack.pop()
        try:
            os.kill(int(pid), signal.SIGTERM)
        except:
            print "WARN - pid:",pid,"not found"


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
    clusterEntityConfigDictList = systemTestEnv.clusterEntityConfigDictList

    for clusterEntityConfigDict in clusterEntityConfigDictList:
        hostname  = clusterEntityConfigDict["hostname"]
        kafkaHome = clusterEntityConfigDict["kafka_home"]
        javaHome  = clusterEntityConfigDict["java_home"]

        localKafkaHome = os.path.abspath(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/..")
        logger.info("local kafka home : [" + localKafkaHome + "]", extra=d)
        if kafkaHome != localKafkaHome:
            logger.error("kafkaHome [" + kafkaHome + "] must be the same as [" + localKafkaHome + "] in host [" + hostname + "]", extra=d)
            logger.error("please update cluster_config.json and run again. Aborting test ...", extra=d)
            sys.exit(1)

        #logger.info("checking running processes in host [" + hostname + "]", extra=d)
        #if not remote_host_processes_stopped(hostname):
        #    logger.error("Running processes found in host [" + hostname + "]", extra=d)
        #    return False

        logger.info("checking JAVA_HOME [" + javaHome + "] in host [" + hostname + "]", extra=d)
        if not remote_host_directory_exists(hostname, javaHome):
            logger.error("Directory not found: [" + javaHome + "] in host [" + hostname + "]", extra=d)
            return False

        logger.info("checking directory [" + kafkaHome + "] in host [" + hostname + "]", extra=d)
        if not remote_host_directory_exists(hostname, kafkaHome):
            logger.info("Directory not found: [" + kafkaHome + "] in host [" + hostname + "]", extra=d)
            if hostname == "localhost":
                return False
            else:
                localKafkaSourcePath = systemTestEnv.SYSTEM_TEST_BASE_DIR + "/.."
                logger.info("copying local copy of [" + localKafkaSourcePath + "] to " + hostname + ":" + kafkaHome, extra=d)
                copy_source_to_remote_hosts(hostname, localKafkaSourcePath, kafkaHome)

    return True

def copy_source_to_remote_hosts(hostname, sourceDir, destDir):

    cmdStr = "rsync -avz --delete-before " + sourceDir + "/ " + hostname + ":" + destDir
    logger.info("executing command [" + cmdStr + "]", extra=d)
    subproc = sys_call_return_subproc(cmdStr)

    for line in subproc.stdout.readlines():
        dummyVar = 1

