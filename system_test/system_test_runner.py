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
#!/usr/bin/evn python

# =================================================================
# system_test_runner.py
#
# - This script is the test driver for a distributed environment
#   system testing framework. It is located at the top level of the
#   framework hierachy (in this case - system_test/).
#
# - This test driver servers as an entry point to launch a series
#   of test suites (module) with multiple functionally similar test
#   cases which can be grouped together.
#
# - Please refer to system_test/README.txt for more details on
#   how to add test suite and test case.
#
# - In most cases, it is not necessary to make any changes to this
#   script.
# =================================================================

from optparse        import OptionParser
from system_test_env import SystemTestEnv
from utils           import system_test_utils

import logging.config
import os
import pprint
import sys


# load the config file for logging
logging.config.fileConfig('logging.conf')

# 'd' is an argument to be merged into the log message (see Python doc for logging).
# In this case, corresponding class name can be appended to the end of the logging
# message to facilitate debugging.
d = {'name_of_class': '(system_test_runner)'}

def main():
    nLogger = logging.getLogger('namedLogger')
    aLogger = logging.getLogger('anonymousLogger')

    optionParser = OptionParser()
    optionParser.add_option("-p", "--print-test-descriptions-only",
                            dest="printTestDescriptionsOnly",
                            default=False,
                            action="store_true",
                            help="print test descriptions only - don't run the test")

    optionParser.add_option("-n", "--do-not-validate-remote-host",
                            dest="doNotValidateRemoteHost",
                            default=False,
                            action="store_true",
                            help="do not validate remote host (due to different kafka versions are installed)")

    (options, args) = optionParser.parse_args()

    print "\n"
    aLogger.info("=================================================")
    aLogger.info("        System Regression Test Framework")
    aLogger.info("=================================================")
    print "\n"

    testSuiteClassDictList = []

    # SystemTestEnv is a class to provide all environement settings for this session
    # such as the SYSTEM_TEST_BASE_DIR, SYSTEM_TEST_UTIL_DIR, ...
    systemTestEnv = SystemTestEnv()

    if options.printTestDescriptionsOnly:
        systemTestEnv.printTestDescriptionsOnly = True
    if options.doNotValidateRemoteHost:
        systemTestEnv.doNotValidateRemoteHost = True

    if not systemTestEnv.printTestDescriptionsOnly:
        if not systemTestEnv.doNotValidateRemoteHost:
            if not system_test_utils.setup_remote_hosts(systemTestEnv):
                nLogger.error("Remote hosts sanity check failed. Aborting test ...", extra=d)
                print
                sys.exit(1)
        else:
            nLogger.info("SKIPPING : checking remote machines", extra=d)
        print

    # get all defined names within a module: 
    definedItemList = dir(SystemTestEnv)
    aLogger.debug("=================================================")
    aLogger.debug("SystemTestEnv keys:")
    for item in definedItemList:
        aLogger.debug("    " + item)
    aLogger.debug("=================================================")

    aLogger.info("=================================================")
    aLogger.info("looking up test suites ...")
    aLogger.info("=================================================")
    # find all test suites in SYSTEM_TEST_BASE_DIR
    for dirName in os.listdir(systemTestEnv.SYSTEM_TEST_BASE_DIR):

        # make sure this is a valid testsuite directory
        if os.path.isdir(dirName) and dirName.endswith(systemTestEnv.SYSTEM_TEST_SUITE_SUFFIX):
            print
            nLogger.info("found a testsuite : " + dirName, extra=d)
            testModulePathName = os.path.abspath(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + dirName)

            if not systemTestEnv.printTestDescriptionsOnly:
                system_test_utils.setup_remote_hosts_with_testsuite_level_cluster_config(systemTestEnv, testModulePathName)

            # go through all test modules file in this testsuite
            for moduleFileName in os.listdir(testModulePathName):

                # make sure it is a valid test module
                if moduleFileName.endswith(systemTestEnv.SYSTEM_TEST_MODULE_EXT) \
                    and not moduleFileName.startswith("__"):

                    # found a test module file
                    nLogger.info("found a test module file : " + moduleFileName, extra=d) 

                    testModuleClassName = system_test_utils.sys_call("grep ^class " + testModulePathName + "/" + \
                                          moduleFileName + " | sed 's/^class //g' | sed 's/(.*):.*//g'")
                    testModuleClassName = testModuleClassName.rstrip('\n')

                    # collect the test suite class data
                    testSuiteClassDict           = {}
                    testSuiteClassDict["suite"]  = dirName
                    extLenToRemove               = systemTestEnv.SYSTEM_TEST_MODULE_EXT.__len__() * -1 
                    testSuiteClassDict["module"] = moduleFileName[:extLenToRemove]
                    testSuiteClassDict["class"]  = testModuleClassName
                    testSuiteClassDictList.append(testSuiteClassDict)

                    suiteName  = testSuiteClassDict["suite"]
                    moduleName = testSuiteClassDict["module"]
                    className  = testSuiteClassDict["class"]

                    # add testsuite directory to sys.path such that the module can be loaded
                    sys.path.append(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + suiteName)
 
                    if not systemTestEnv.printTestDescriptionsOnly:
                        aLogger.info("=================================================")
                        aLogger.info("Running Test for : ")
                        aLogger.info("    suite  : " + suiteName)
                        aLogger.info("    module : " + moduleName)
                        aLogger.info("    class  : " + className)
                        aLogger.info("=================================================")

                    # dynamically loading a module and starting the test class
                    mod      = __import__(moduleName)
                    theClass = getattr(mod, className)
                    instance = theClass(systemTestEnv)
                    instance.runTest()
                print

    if not systemTestEnv.printTestDescriptionsOnly:
        totalFailureCount = 0
        print
        print "========================================================"
        print "                 TEST REPORTS"
        print "========================================================"
        for systemTestResult in systemTestEnv.systemTestResultsList:
            for key in sorted(systemTestResult.iterkeys()):
                if key == "validation_status":
                    print key, " : "
                    testItemStatus = None
                    for validatedItem in sorted(systemTestResult[key].iterkeys()):
                        testItemStatus = systemTestResult[key][validatedItem]
                        print "    ", validatedItem, " : ", testItemStatus
                        if "FAILED" == testItemStatus:
                            totalFailureCount += 1
                else:
                    print key, " : ", systemTestResult[key]
            print
            print "========================================================"
            print

        print "========================================================"
        print "Total failures count : " + str(totalFailureCount)
        print "========================================================"
        print
        return totalFailureCount

    return -1

# =========================
# main entry point
# =========================

main()


