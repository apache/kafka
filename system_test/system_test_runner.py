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

class report:
  systemTestEnv = None
  reportString = ""
  reportFileName = "system_test_report.html"
  systemTestReport = None
  header = """<head>
  <title>Kafka System Test Report</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="http://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js"></script>
  <script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"></script>
  </head>"""
  footer = """ """

  def __init__(self, systemTestEnv):
    self.totalSkipped = 0
    self.totalPassed = 0
    self.totalTests = 0
    self.totalFailed = 0
    self.systemTestEnv = systemTestEnv
    self.systemTestReport = open(self.reportFileName, 'w')

  def __del__(self):
    self.systemTestReport.close()
    self.systemTestReport = None

  def writeHtmlPage(self, body):
    html = """
    <!DOCTYPE html>
    <html lang="en">
    """
    html += self.header
    html += body
    html += self.footer
    html += """
    </html>
    """
    self.systemTestReport.write(html)

  def wrapIn(self, tag, content):
    html = "\n<" + tag + ">"
    html += "\n  " + content
    html += "\n</" + tag.split(" ")[0] + ">"
    return html

  def genModal(self, className, caseName, systemTestResult):
    key = "validation_status"
    id = className + "_" + caseName
    info = self.wrapIn("h4", "Validation Status")
    for validatedItem in sorted(systemTestResult[key].iterkeys()):
      testItemStatus = systemTestResult[key][validatedItem]
      info += validatedItem + " : " + testItemStatus
    return self.wrapIn("div class=\"modal fade\" id=\"" + id + "\" tabindex=\"-1\" role=\"dialog\" aria-labelledby=\"" + id + "Label\" aria-hidden=\"true\"",
                       self.wrapIn("div class=\"modal-dialog\"",
                                   self.wrapIn("div class=\"modal-content\"",
                                               self.wrapIn("div class=\"modal-header\"",
                                                           self.wrapIn("h4 class=\"modal-title\" id=\"" + id + "Label\"",
                                                                       className + " - " + caseName)) +
                                                self.wrapIn("div class=\"modal-body\"",
                                                            info) +
                                                self.wrapIn("div class=\"modal-footer\"",
                                                            self.wrapIn("button type=\"button\" class=\"btn btn-default\" data-dismiss=\"modal\"", "Close")))))

  def summarize(self):
    testItemsTableHeader = self.wrapIn("thead",
                                       self.wrapIn("tr",
                                                   self.wrapIn("th", "Test Class Name") +
                                                   self.wrapIn("th", "Test Case Name") +
                                                   self.wrapIn("th", "Validation Status")))
    testItemsTableBody = ""
    modals = ""

    for systemTestResult in self.systemTestEnv.systemTestResultsList:
      self.totalTests += 1
      if "_test_class_name" in systemTestResult:
        testClassName = systemTestResult["_test_class_name"]
      else:
        testClassName = ""

      if "_test_case_name" in systemTestResult:
        testCaseName = systemTestResult["_test_case_name"]
      else:
        testCaseName = ""

      if "validation_status" in systemTestResult:
        testItemStatus = "SKIPPED"
        for key in systemTestResult["validation_status"].iterkeys():
          testItemStatus = systemTestResult["validation_status"][key]
          if "FAILED" == testItemStatus:
            break;
        if "FAILED" == testItemStatus:
          self.totalFailed += 1
          validationStatus = self.wrapIn("div class=\"text-danger\" data-toggle=\"modal\" data-target=\"#" + testClassName + "_" + testCaseName + "\"", "FAILED")
          modals += self.genModal(testClassName, testCaseName, systemTestResult)
        elif "PASSED" == testItemStatus:
          self.totalPassed += 1
          validationStatus = self.wrapIn("div class=\"text-success\"", "PASSED")
        else:
          self.totalSkipped += 1
          validationStatus = self.wrapIn("div class=\"text-warning\"", "SKIPPED")
      else:
        self.reportString += "|"

      testItemsTableBody += self.wrapIn("tr",
                                       self.wrapIn("td", testClassName) +
                                       self.wrapIn("td", testCaseName) +
                                       self.wrapIn("td", validationStatus))

    testItemsTableBody = self.wrapIn("tbody", testItemsTableBody)
    testItemsTable = self.wrapIn("table class=\"table table-striped\"", testItemsTableHeader + testItemsTableBody)

    statsTblBody = self.wrapIn("tr class=\"active\"", self.wrapIn("td", "Total tests") + self.wrapIn("td", str(self.totalTests)))
    statsTblBody += self.wrapIn("tr class=\"success\"", self.wrapIn("td", "Total tests passed") + self.wrapIn("td", str(self.totalPassed)))
    statsTblBody += self.wrapIn("tr class=\"danger\"", self.wrapIn("td", "Total tests failed") + self.wrapIn("td", str(self.totalFailed)))
    statsTblBody += self.wrapIn("tr class=\"warning\"", self.wrapIn("td", "Total tests skipped") + self.wrapIn("td", str(self.totalSkipped)))
    testStatsTable = self.wrapIn("table class=\"table\"", statsTblBody)

    body = self.wrapIn("div class=\"container\"",
                          self.wrapIn("h2", "Kafka System Test Report") +
                          self.wrapIn("div class=\"row\"", self.wrapIn("div class=\"col-md-4\"", testStatsTable)) +
                          self.wrapIn("div class=\"row\"", self.wrapIn("div class=\"col-md-6\"", testItemsTable)) +
                          modals)
    self.writeHtmlPage(self.wrapIn("body", body))

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

    report(systemTestEnv).summarize()

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

sys.exit(main())


