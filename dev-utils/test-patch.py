#!/usr/bin/env python
#
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
#

#
# Pre Commit Hook for running tests and updating JIRA
#
# Original version was copied from SQOOP project.
#
import sys, os, re, urllib2, base64, subprocess, tempfile, shutil
import json
import datetime
from optparse import OptionParser

tmp_dir = None
BASE_JIRA_URL = 'https://issues.apache.org/jira'
BRANCHES = ["trunk", "0.7", "0.7.0", "0.7.1", "0.7.2", "0.8", "0.8.1", "0.8.2"]

# Write output to file
def write_file(filename, content):
  with open(filename, "w") as text_file:
      text_file.write(content)

# Guess branch for given versions
#
# Return None if detects that JIRA belongs to more than one branch
def kafka_guess_branch(versions):
  if not versions:
    return BRANCHES[0]

  for version in versions:
    for branch in BRANCHES:
      if version == branch:
        return branch

  return BRANCHES[0]

# Verify supported branch
def kafka_verify_branch(branch):
  return branch in BRANCHES

def execute(cmd, log=True):
  if log:
    print "INFO: Executing %s" % (cmd)
  return subprocess.call(cmd, shell=True)

def jenkins_link_for_jira(name, endpoint):
  if "BUILD_URL" in os.environ:
    return "[%s|%s%s]" % (name, os.environ['BUILD_URL'], endpoint)
  else:
    return name

def jenkins_file_link_for_jira(name, file):
  return jenkins_link_for_jira(name, "artifact/patch-process/%s" % file)

def jira_request(result, url, username, password, data, headers):
  request = urllib2.Request(url, data, headers)
  print "INFO: URL = %s, Username = %s, data = %s, headers = %s" % (url, username, data, str(headers))
  if username and password:
    base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    request.add_header("Authorization", "Basic %s" % base64string)
  return urllib2.urlopen(request)

def jira_get_defect_html(result, defect, username, password):
  url = "%s/browse/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_get_defect(result, defect, username, password):
  url = "%s/rest/api/2/issue/%s" % (BASE_JIRA_URL, defect)
  return jira_request(result, url, username, password, None, {}).read()

def jira_generate_comment(result, branch):
  body =  [ "Testing file [%s|%s] against branch %s took %s." % (result.attachment.split('/')[-1] , result.attachment, branch, datetime.datetime.now() - result.start_time) ]
  body += [ "" ]
  if result._fatal:
    result._error = [ result._fatal ] + result._error
  if result._error:
    count = len(result._error)
    if count == 1:
      body += [ "{color:red}Overall:{color} -1 due to an error" ]
    else:
      body += [ "{color:red}Overall:{color} -1 due to %d errors" % (count) ]
  else:
    body += [ "{color:green}Overall:{color} +1 all checks pass" ]
  body += [ "" ]
  for error in result._error:
    body += [ "{color:red}ERROR:{color} %s" % (error.replace("\n", "\\n")) ]
  for info in result._info:
    body += [ "INFO: %s" % (info.replace("\n", "\\n")) ]
  for success in result._success:
    body += [ "{color:green}SUCCESS:{color} %s" % (success.replace("\n", "\\n")) ]
  if "BUILD_URL" in os.environ:
    body += [ "" ]
    body += [ "Console output is available %s." % (jenkins_link_for_jira("here", "console")) ]
  body += [ "" ]
  body += [ "This message is automatically generated." ]
  return "\\n".join(body)

def jira_post_comment(result, defect, branch, username, password):
  url = "%s/rest/api/2/issue/%s/comment" % (BASE_JIRA_URL, defect)

  # Generate body for the comment and save it to a file
  body = jira_generate_comment(result, branch)
  write_file("%s/jira-comment.txt" % output_dir, body.replace("\\n", "\n"))

  # Send the comment to the JIRA
  body = "{\"body\": \"%s\"}" % body
  headers = {'Content-Type' : 'application/json'}
  response = jira_request(result, url, username, password, body, headers)
  body = response.read()
  if response.code != 201:
    msg = """Request for %s failed:
  URL = '%s'
  Code = '%d'
  Comment = '%s'
  Response = '%s'
    """ % (defect, url, response.code, comment, body)
    print "FATAL: %s" % (msg)
    sys.exit(1)

# hack (from hadoop) but REST api doesn't list attachments?
def jira_get_attachment(result, defect, username, password):
  html = jira_get_defect_html(result, defect, username, password)
  escaped_colon = re.escape("%3A")
  pattern = "(/secure/attachment/[0-9]+/(bug)?%s[0-9\-]*((\.|-)v?[0-9]+)?\.(patch|txt|patch\.txt))" % (re.escape(defect))
  kafka_pattern = "(/secure/attachment/[0-9]+/(bug)?%s_[0-9]+-[0-9]+-[0-9]+_[0-9]+%s[0-9]+%s[0-9]+[0-9\-]*((\.|-)v?[0-9]+)?\.(patch|txt|patch\.txt))" % (re.escape(defect), escaped_colon, escaped_colon)
  matches = []
  for match in re.findall(kafka_pattern, html, re.IGNORECASE) or re.findall(pattern, html, re.IGNORECASE):
    matches += [ match[0] ]
  if matches:
    matches.sort()
    return  "%s%s" % (BASE_JIRA_URL, matches.pop())
  return None

# Get versions from JIRA JSON object
def json_get_version(json):
  versions = []

  # Load affectedVersion field
  for version in json.get("fields").get("versions"):
    versions = versions + [version.get("name").strip()]

  # Load fixVersion field
  for version in json.get("fields").get("fixVersions"):
    versions = versions + [version.get("name").strip()]

  if not versions:
    print "No Affected or Fixed version found in JIRA"

  return versions

def git_cleanup():
  rc = execute("git clean -d -f", False)
  if rc != 0:
    print "ERROR: git clean failed"
  rc = execute("git reset --hard HEAD", False)
  if rc != 0:
    print "ERROR: git reset failed"

def git_checkout(result, branch):
  if not branch:
    result.fatal("Branch wasn't specified nor was correctly guessed")
    return

  if execute("git checkout %s" % (branch)) != 0:
    result.fatal("git checkout %s failed" % branch)
  if execute("git clean -d -f") != 0:
    result.fatal("git clean failed")
  if execute("git reset --hard HEAD") != 0:
    result.fatal("git reset failed")
  if execute("git fetch origin") != 0:
    result.fatal("git fetch failed")
  if execute("git merge --ff-only origin/%s" % (branch)):
    result.fatal("git merge failed")

def git_apply(result, cmd, patch_file, strip, output_dir):
  output_file = "%s/apply.txt" % (output_dir)
  rc = execute("%s -p%s < %s 1>%s 2>&1" % (cmd, strip, patch_file, output_file))
  output = ""
  if os.path.exists(output_file):
    with open(output_file) as fh:
      output = fh.read()
  if rc == 0:
    if output:
      result.success("Patch applied, but there has been warnings:\n{code}%s{code}\n" % (output))
    else:
      result.success("Patch applied correctly")
  else:
    result.fatal("failed to apply patch (exit code %d):\n{code}%s{code}\n" % (rc, output))

def static_test(result, patch_file, output_dir):
  output_file = "%s/static-test.txt" % (output_dir)
  rc = execute("grep '^+++.*/test' %s 1>%s 2>&1" % (patch_file, output_file))
  if rc == 0:
    result.success("Patch add/modify test case")
  else:
    result.error("Patch does not add/modify any test case")

def gradle_bootstrap(result, output_dir):
  rc = execute("gradle 1>%s/bootstrap.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Gradle bootstrap was successful")
  else:
    result.fatal("failed to bootstrap project (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "bootstrap.txt")))

def gradle_clean(result, output_dir):
  rc = execute("./gradlew clean 1>%s/clean.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Clean was successful")
  else:
    result.fatal("failed to clean project (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "clean.txt")))

def gradle_install(result, output_dir):
  rc = execute("./gradlew jarAll 1>%s/install.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Patch compiled")
  else:
    result.fatal("failed to build with patch (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "install.txt")))

def checkstyleMain(result, output_dir):
  rc = execute("./gradlew checkstyleMain 1>%s/checkstyleMain.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Checked style for Main")
  else:
    result.fatal("checkstyleMain failed with patch (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "checkstyleMain.txt")))

def checkstyleTest(result, output_dir):
  rc = execute("./gradlew checkstyleTest 1>%s/checkstyleTest.txt 2>&1" % output_dir)
  if rc == 0:
    result.success("Checked style for Test")
  else:
    result.fatal("checkstyleTest failed with patch (exit code %d, %s)" % (rc, jenkins_file_link_for_jira("report", "checkstyleTest.txt")))

def gradle_test(result, output_dir):
  run_gradle_test("testAll", "unit", result, output_dir)

def run_gradle_test(command, test_type, result, output_dir):
  rc = execute("./gradlew %s 1>%s/test_%s.txt 2>&1" % (command, output_dir, test_type))
  if rc == 0:
    result.success("All %s tests passed" % test_type)
  else:
    result.error("Some %s tests failed (%s)" % (test_type, jenkins_file_link_for_jira("report", "test_%s.txt" % test_type)))
    failed_tests = []
    fd = open("%s/test_%s.txt" % (output_dir, test_type), "r")
    for line in fd:
      if "FAILED" in line and " > " in line:
        failed_tests += [line]
    fd.close()
    for failed_test in set(failed_tests):
      result.error("Failed %s test: {{%s}}" % (test_type, failed_test))

def clean_folder(folder):
  for the_file in os.listdir(folder):
      file_path = os.path.join(folder, the_file)
      try:
          if os.path.isfile(file_path):
              os.unlink(file_path)
      except Exception, e:
          print e

class Result(object):
  def __init__(self):
    self._error = []
    self._info = []
    self._success = []
    self._fatal = None
    self.exit_handler = None
    self.attachment = "Not Found"
    self.start_time = datetime.datetime.now()
  def error(self, msg):
    self._error.append(msg)
  def info(self, msg):
    self._info.append(msg)
  def success(self, msg):
    self._success.append(msg)
  def fatal(self, msg):
    self._fatal = msg
    self.exit_handler()
    self.exit()
  def exit(self):
    git_cleanup()
    global tmp_dir
    global copy_output_dir
    global output_dir
    if copy_output_dir:
      print "INFO: Moving output to %s" % (copy_output_dir)
      os.renames(output_dir, copy_output_dir)
      tmp_dir = None
    if tmp_dir:
      print "INFO: output is located %s" % (tmp_dir)
    sys.exit(0)

usage = "usage: %prog [options]"
parser = OptionParser(usage)
parser.add_option("--branch", dest="branch",
                  help="Local git branch to test against", metavar="trunk")
parser.add_option("--defect", dest="defect",
                  help="Defect name", metavar="KAFKA-1856")
parser.add_option("--file", dest="filename",
                  help="Test patch file", metavar="FILE")
parser.add_option("--run-tests", dest="run_tests",
                  help="Run Tests", action="store_true")
parser.add_option("--username", dest="username",
                  help="JIRA Username", metavar="USERNAME", default="kafkaqa")
parser.add_option("--output", dest="output_dir",
                  help="Directory to write output", metavar="DIRECTORY")
parser.add_option("--post-results", dest="post_results",
                  help="Post results to JIRA (only works in defect mode)", action="store_true")
parser.add_option("--password", dest="password",
                  help="JIRA Password", metavar="PASSWORD")
parser.add_option("--patch-command", dest="patch_cmd", default="git apply",
                  help="Patch command such as `git apply' or `patch'", metavar="COMMAND")
parser.add_option("-p", "--strip", dest="strip", default="1",
                  help="Remove <n> leading slashes from diff paths", metavar="N")

(options, args) = parser.parse_args()
if not (options.defect or options.filename):
  print "FATAL: Either --defect or --file is required."
  sys.exit(1)

if options.defect and options.filename:
  print "FATAL: Both --defect and --file cannot be specified."
  sys.exit(1)

if options.post_results and not options.password:
  print "FATAL: --post-results requires --password"
  sys.exit(1)

branch = options.branch
if options.output_dir and not options.output_dir.startswith('/'):
  print "INFO: A temporary staging dir for output will be used to avoid deletion of output files during 'git reset'"
  copy_output_dir = options.output_dir
  output_dir = None
else:
  output_dir = options.output_dir
  copy_output_dir = None
defect = options.defect
username = options.username
password = options.password
run_tests = options.run_tests
post_results = options.post_results
strip = options.strip
patch_cmd = options.patch_cmd
result = Result()

if output_dir and os.path.isdir(output_dir):
  clean_folder(output_dir)
if copy_output_dir and os.path.isdir(copy_output_dir):
  clean_folder(copy_output_dir)

# Default exit handler in case that we do not want to submit results to JIRA
def log_and_exit():
  # Write down comment generated for jira (won't be posted)
  write_file("%s/jira-comment.txt" % output_dir, jira_generate_comment(result, branch).replace("\\n", "\n"))

  if result._fatal:
    print "FATAL: %s" % (result._fatal)
  for error in result._error:
    print "ERROR: %s" % (error)
  for info in result._info:
    print "INFO: %s" % (info)
  for success in result._success:
    print "SUCCESS: %s" % (success)
  result.exit()

result.exit_handler = log_and_exit

if post_results:
  def post_jira_comment_and_exit():
    jira_post_comment(result, defect, branch, username, password)
    result.exit()
  result.exit_handler = post_jira_comment_and_exit

if not output_dir:
  tmp_dir = tempfile.mkdtemp()
  output_dir = tmp_dir

if output_dir.endswith("/"):
  output_dir = output_dir[:-1]

if output_dir and not os.path.isdir(output_dir):
  os.makedirs(output_dir)

# If defect parameter is specified let's download the latest attachment
if defect:
  print "Defect: %s" % defect
  jira_json = jira_get_defect(result, defect, username, password)
  json = json.loads(jira_json)

  # JIRA must be in Patch Available state
  if '"Patch Available"' not in jira_json:
    print "ERROR: Defect %s not in patch available state" % (defect)
    sys.exit(1)

  # If branch is not specified, let's try to guess it from JIRA details
  if not branch:
    versions = json_get_version(json)
    branch = kafka_guess_branch(versions)
    if not branch:
      print "ERROR: Can't guess branch name from %s" % (versions)
      sys.exit(1)
    else:
      print "INFO: Guessed branch as %s" % (branch)

  attachment = jira_get_attachment(result, defect, username, password)
  if not attachment:
    print "ERROR: No attachments found for %s" % (defect)
    sys.exit(1)

  result.attachment = attachment

  patch_contents = jira_request(result, result.attachment, username, password, None, {}).read()
  patch_file = "%s/%s.patch" % (output_dir, defect)

  with open(patch_file, 'a') as fh:
    fh.write(patch_contents)
elif options.filename:
  patch_file = options.filename
else:
  raise Exception("Not reachable")

# Verify that we are on supported branch
if not kafka_verify_branch(branch):
  print "ERROR: Unsupported branch %s" % (branch)
  sys.exit(1)

gradle_bootstrap(result, output_dir)
gradle_clean(result, output_dir)
git_checkout(result, branch)
git_apply(result, patch_cmd, patch_file, strip, output_dir)
static_test(result, patch_file, output_dir)
gradle_bootstrap(result, output_dir)
gradle_install(result, output_dir)
checkstyleMain(result, output_dir)
checkstyleTest(result, output_dir)
if run_tests:
  gradle_test(result, output_dir)
else:
  result.info("patch applied and built but tests did not execute")

result.exit_handler()
