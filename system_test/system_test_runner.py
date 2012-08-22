#!/usr/bin/evn python

# ===================================
# system_test_runner.py
# ===================================

from system_test_env import SystemTestEnv
from utils import system_test_utils

import logging
import os
import sys


# ====================================================================
# Two logging formats are defined in system_test/system_test_runner.py
# ====================================================================

# 1. "namedLogger" is defined to log message in this format:
#    "%(asctime)s - %(levelname)s - %(message)s %(name_of_class)s"
# 
# usage: to log message and showing the class name of the message

namedLogger = logging.getLogger("namedLogger")
namedLogger.setLevel(logging.INFO)
#namedLogger.setLevel(logging.DEBUG)
namedConsoleHandler = logging.StreamHandler()
namedConsoleHandler.setLevel(logging.DEBUG)
namedFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s %(name_of_class)s")
namedConsoleHandler.setFormatter(namedFormatter)
namedLogger.addHandler(namedConsoleHandler)

# 2. "anonymousLogger" is defined to log message in this format:
#    "%(asctime)s - %(levelname)s - %(message)s"
# 
# usage: to log message without showing class name and it's appropriate
#        for logging generic message such as "sleeping for 5 seconds"

anonymousLogger = logging.getLogger("anonymousLogger")
anonymousLogger.setLevel(logging.INFO)
#anonymousLogger.setLevel(logging.DEBUG)
anonymousConsoleHandler = logging.StreamHandler()
anonymousConsoleHandler.setLevel(logging.DEBUG)
anonymousFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
anonymousConsoleHandler.setFormatter(anonymousFormatter)
anonymousLogger.addHandler(anonymousConsoleHandler)

d = {'name_of_class': '(system_test_runner)'}

def main():

    print
    print
    print
    anonymousLogger.info("=================================================")
    anonymousLogger.info("        System Regression Test Framework")
    anonymousLogger.info("=================================================")
    print
    print

    testSuiteClassDictList = []

    # SystemTestEnv is a class to provide all environement settings for this session
    # such as the SYSTEM_TEST_BASE_DIR, SYSTEM_TEST_UTIL_DIR, ...
    systemTestEnv = SystemTestEnv()

    # sanity check on remote hosts to make sure:
    # - all directories (eg. java_home) specified in cluster_config.json exists in all hosts
    # - no conflicting running processes in remote hosts
    anonymousLogger.info("=================================================")
    anonymousLogger.info("setting up remote hosts ...")
    anonymousLogger.info("=================================================")
    if not system_test_utils.setup_remote_hosts(systemTestEnv):
        namedLogger.error("Remote hosts sanity check failed. Aborting test ...", extra=d)
        print
        sys.exit(1)
    print

    # get all defined names within a module: 
    definedItemList = dir(SystemTestEnv)
    anonymousLogger.debug("=================================================")
    anonymousLogger.debug("SystemTestEnv keys:")
    for item in definedItemList:
        anonymousLogger.debug("    " + item)
    anonymousLogger.debug("=================================================")

    anonymousLogger.info("=================================================")
    anonymousLogger.info("looking up test suites ...")
    anonymousLogger.info("=================================================")
    # find all test suites in SYSTEM_TEST_BASE_DIR
    for dirName in os.listdir(systemTestEnv.SYSTEM_TEST_BASE_DIR):

        # make sure this is a valid testsuite directory
        if os.path.isdir(dirName) and dirName.endswith(systemTestEnv.SYSTEM_TEST_SUITE_SUFFIX):
            
            namedLogger.info("found a testsuite : " + dirName, extra=d)
            testModulePathName = os.path.abspath(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + dirName)

            # go through all test modules file in this testsuite
            for moduleFileName in os.listdir(testModulePathName):

                # make sure it is a valid test module
                if moduleFileName.endswith(systemTestEnv.SYSTEM_TEST_MODULE_EXT) \
                   and not moduleFileName.startswith("__"):

                    # found a test module file
                    namedLogger.info("found a test module file : " + moduleFileName, extra=d) 

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

    # loop through testSuiteClassDictList and start the test class one by one
    for testSuiteClassDict in testSuiteClassDictList:

        suiteName  = testSuiteClassDict["suite"]
        moduleName = testSuiteClassDict["module"]
        className  = testSuiteClassDict["class"]

        # add testsuite directory to sys.path such that the module can be loaded
        sys.path.append(systemTestEnv.SYSTEM_TEST_BASE_DIR + "/" + suiteName)

        anonymousLogger.info("=================================================")
        anonymousLogger.info("Running Test for : ")
        anonymousLogger.info("    suite  : " + suiteName)
        anonymousLogger.info("    module : " + moduleName)
        anonymousLogger.info("    class  : " + className)
        anonymousLogger.info("=================================================")

        # dynamically loading a module and starting the test class
        mod      = __import__(moduleName)
        theClass = getattr(mod, className)
        instance = theClass(systemTestEnv)
        instance.runTest()

    print
    anonymousLogger.info("=================================================")
    anonymousLogger.info("                 TEST REPORTS")
    anonymousLogger.info("=================================================")
    for systemTestResult in systemTestEnv.systemTestResultsList:
        for key,val in systemTestResult.items():
            if key == "validation_status":
                anonymousLogger.info(key + " : ")
                for validation, status in val.items():
                     anonymousLogger.info("    " + validation + " : " + status)
            else:
                anonymousLogger.info(key + " : " + val)
        print

# =========================
# main entry point
# =========================

main()


