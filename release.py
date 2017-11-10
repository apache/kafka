#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Utility for creating release candidates and promoting release candidates to a final relase.

Usage: release.py

The utility is interactive; you will be prompted for basic release information and guided through the process.

This utility assumes you already have local a kafka git folder and that you
have added remotes corresponding to both:
(i) the github apache kafka mirror and
(ii) the apache kafka git repo.
"""

from __future__ import print_function

import datetime
from getpass import getpass
import json
import os
import subprocess
import sys
import tempfile

PROJECT_NAME = "kafka"
CAPITALIZED_PROJECT_NAME = "kafka".upper()
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
# Location of the local git repository
REPO_HOME = os.environ.get("%s_HOME" % CAPITALIZED_PROJECT_NAME, SCRIPT_DIR)
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
PREFS_FILE = os.path.join(SCRIPT_DIR, '.release-settings.json')

delete_gitrefs = False
work_dir = None

def fail(msg):
    if work_dir:
        cmd("Cleaning up work directory", "rm -rf %s" % work_dir)

    if delete_gitrefs:
        try:
            cmd("Resetting repository working state to branch %s" % starting_branch, "git reset --hard HEAD && git checkout %s" % starting_branch, shell=True)
            cmd("Deleting git branches %s" % release_version, "git branch -D %s" % release_version, shell=True)
            cmd("Deleting git tag %s" %rc_tag , "git tag -d %s" % rc_tag, shell=True)
        except subprocess.CalledProcessError:
            print("Failed when trying to clean up git references added by this script. You may need to clean up branches/tags yourself before retrying.")
            print("Expected git branch: " + release_version)
            print("Expected git tag: " + rc_tag)
    print(msg)
    sys.exit(1)

def print_output(output):
    if output is None or len(output) == 0:
        return
    for line in output.split('\n'):
        print(">", line)

def cmd(action, cmd, *args, **kwargs):
    if isinstance(cmd, basestring) and not kwargs.get("shell", False):
        cmd = cmd.split()

    stdin_log = ""
    if "stdin" in kwargs and isinstance(kwargs["stdin"], basestring):
        stdin_log = "--> " + kwargs["stdin"]
        stdin = tempfile.TemporaryFile()
        stdin.write(kwargs["stdin"])
        stdin.seek(0)
        kwargs["stdin"] = stdin

    print(action, cmd, stdin_log)
    try:
        output = subprocess.check_output(cmd, *args, stderr=subprocess.STDOUT, **kwargs)
        print_output(output)
    except subprocess.CalledProcessError as e:
        print_output(e.output)

        print("*************************************************")
        print("*** First command failure occurred here.      ***")
        print("*** Will now try to clean up working state.   ***")
        print("*************************************************")
        fail("")


def cmd_output(cmd, *args, **kwargs):
    if isinstance(cmd, basestring):
        cmd = cmd.split()
    return subprocess.check_output(cmd, *args, stderr=subprocess.STDOUT, **kwargs)

def replace(path, pattern, replacement):
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append((replacement + '\n') if line.startswith(pattern) else line)

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)

def user_ok(msg):
    ok = raw_input(msg)
    return ok.lower() == 'y'

def sftp_mkdir(dir):
    basedir, dirname = os.path.split(dir)
    if not basedir:
       basedir = "."
    try:
       cmd_str  = """
cd %s
mkdir %s
""" % (basedir, dirname)
       cmd("Creating '%s' in '%s' in your Apache home directory if it does not exist (errors are ok if the directory already exists)" % (dirname, basedir), "sftp -b - %s@home.apache.org" % apache_id, stdin=cmd_str)
    except subprocess.CalledProcessError:
        # This is ok. The command fails if the directory already exists
        pass

def get_pref(prefs, name, request_fn):
    "Get a preference from existing preference dictionary or invoke a function that can collect it from the user"
    val = prefs.get(name)
    if not val:
        val = request_fn()
        prefs[name] = val
    return val

# Load saved preferences
prefs = {}
if os.path.exists(PREFS_FILE):
    with open(PREFS_FILE, 'r') as prefs_fp:
        prefs = json.load(prefs_fp)

if not user_ok("""Requirements:
1. Updated docs to reference the new release version where appropriate.
2. JDK7 and JDK8 compilers and libraries
3. Your Apache ID, already configured with SSH keys on id.apache.org and SSH keys available in this shell session
4. All issues in the target release resolved with valid resolutions (if not, this script will report the problematic JIRAs)
5. A GPG key used for signing the release. This key should have been added to public Apache servers and the KEYS file on the Kafka site
6. Standard toolset installed -- git, gpg, gradle, sftp, etc.
7. ~/.gradle/gradle.properties configured with the signing properties described in the release process wiki, i.e.

      mavenUrl=https://repository.apache.org/service/local/staging/deploy/maven2
      mavenUsername=your-apache-id
      mavenPassword=your-apache-passwd
      signing.keyId=your-gpgkeyId
      signing.password=your-gpg-passphrase
      signing.secretKeyRingFile=/Users/your-id/.gnupg/secring.gpg (if you are using GPG 2.1 and beyond, then this file will no longer exist anymore, and you have to manually create it from the new private key directory with "gpg --export-secret-keys -o ~/.gnupg/secring.gpg")
8. ~/.m2/settings.xml configured for pgp signing and uploading to apache release maven, i.e., 
       <server>
          <id>apache.releases.https</id>
          <username>your-apache-id</username>
          <password>your-apache-passwd</password>
        </server>
	<server>
            <id>your-gpgkeyId</id>
            <passphrase>your-gpg-passphase</passphrase>
        </server>
        <profile>
            <id>gpg-signing</id>
            <properties>
                <gpg.keyname>your-gpgkeyId</gpg.keyname>
        	<gpg.passphraseServerId>your-gpgkeyId</gpg.passphraseServerId>
            </properties>
        </profile>
9. You may also need to update some gnupgp configs:
	~/.gnupg/gpg-agent.conf
	allow-loopback-pinentry

	~/.gnupg/gpg.conf
	use-agent
	pinentry-mode loopback

	echo RELOADAGENT | gpg-connect-agent

If any of these are missing, see https://cwiki.apache.org/confluence/display/KAFKA/Release+Process for instructions on setting them up.

Some of these may be used from these previous settings loaded from %s:

%s

Do you have all of of these setup? (y/n): """ % (PREFS_FILE, json.dumps(prefs, indent=2))):
    fail("Please try again once you have all the prerequisites ready.")


starting_branch = cmd_output('git rev-parse --abbrev-ref HEAD')

cmd("Verifying that you have no unstaged git changes", 'git diff --exit-code --quiet')
cmd("Verifying that you have no staged git changes", 'git diff --cached --exit-code --quiet')

release_version = raw_input("Release version (without any RC info, e.g. 1.0.0): ")
try:
    release_version_parts = release_version.split('.')
    if len(release_version_parts) != 3:
        fail("Invalid release version, should have 3 version number components")
    # Validate each part is a number
    [int(x) for x in release_version_parts]
except ValueError:
    fail("Invalid release version, should be a dotted version number")

rc = raw_input("Release candidate number: ")

dev_branch = '.'.join(release_version_parts[:2])
docs_version = ''.join(release_version_parts[:2])

# Validate that the release doesn't already exist and that the
cmd("Fetching tags from upstream", 'git fetch --tags %s' % PUSH_REMOTE_NAME)
tags = cmd_output('git tag').split()

if release_version in tags:
    fail("The specified version has already been tagged and released.")

# TODO promotion
if not rc:
    fail("Automatic Promotion is not yet supported.")

    # Find the latest RC and make sure they want to promote that one
    rc_tag = sorted([t for t in tags if t.startswith(release_version + '-rc')])[-1]
    if not user_ok("Found %s as latest RC for this release. Is this correct? (y/n): "):
        fail("This script couldn't determine which RC tag to promote, you'll need to fix up the RC tags and re-run the script.")

    sys.exit(0)

# Prereq checks
apache_id = get_pref(prefs, 'apache_id', lambda: raw_input("Enter your apache username: "))


jdk7_java_home = get_pref(prefs, 'jdk7', lambda: raw_input("Enter the path for JAVA_HOME for a JDK7 compiler (blank to use default JAVA_HOME): "))
jdk7_env = dict(os.environ) if jdk7_java_home.strip() else None
if jdk7_env is not None: jdk7_env['JAVA_HOME'] = jdk7_java_home
if "1.7.0" not in cmd_output("java -version", env=jdk7_env):
    fail("You must be able to build artifacts with JDK7 for Scala 2.10 and 2.11 artifacts")

jdk8_java_home = get_pref(prefs, 'jdk8', lambda: raw_input("Enter the path for JAVA_HOME for a JDK8 compiler (blank to use default JAVA_HOME): "))
jdk8_env = dict(os.environ) if jdk8_java_home.strip() else None
if jdk8_env is not None: jdk8_env['JAVA_HOME'] = jdk8_java_home
if "1.8.0" not in cmd_output("java -version", env=jdk8_env):
    fail("You must be able to build artifacts with JDK8 for Scala 2.12 artifacts")


def select_gpg_key():
    print("Here are the available GPG keys:")
    available_keys = cmd_output("gpg --list-secret-keys")
    print(available_keys)
    key_name = raw_input("Which user name (enter the user name without email address): ")
    if key_name not in available_keys:
        fail("Couldn't find the requested key.")
    return key_name

key_name = get_pref(prefs, 'gpg-key', select_gpg_key)

gpg_passphrase = get_pref(prefs, 'gpg-pass', lambda: getpass("Passphrase for this GPG key: "))
# Do a quick validation so we can fail fast if the password is incorrect
with tempfile.NamedTemporaryFile() as gpg_test_tempfile:
    gpg_test_tempfile.write("abcdefg")
    cmd("Testing GPG key & passphrase", ["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase-fd", "0", "-u", key_name, "--armor", "--output", gpg_test_tempfile.name + ".asc", "--detach-sig", gpg_test_tempfile.name], stdin=gpg_passphrase)

# Save preferences
print("Saving preferences to %s" % PREFS_FILE)
with open(PREFS_FILE, 'w') as prefs_fp:
    prefs = json.dump(prefs, prefs_fp)

# Generate RC
try:
    int(rc)
except ValueError:
    fail("Invalid release candidate number: %s" % rc)
rc_tag = release_version + '-rc' + rc

delete_gitrefs = True # Since we are about to start creating new git refs, enable cleanup function on failure to try to delete them
cmd("Checking out current development branch", "git checkout -b %s %s" % (release_version, PUSH_REMOTE_NAME + "/" + dev_branch))
print("Updating version numbers")
replace("gradle.properties", "version", "version=%s" % release_version)
replace("tests/kafkatest/__init__.py", "__version__", "__version__ = '%s'" % release_version)
cmd("update streams quickstart pom", ["sed", "-i", ".orig"," s/-SNAPSHOT//", "streams/quickstart/pom.xml"])
cmd("update streams quickstart java pom", ["sed", "-i", ".orig", "s/-SNAPSHOT//", "streams/quickstart/java/pom.xml"])
cmd("update streams quickstart java pom", ["sed", "-i", ".orig", "s/-SNAPSHOT//", "streams/quickstart/java/src/main/resources/archetype-resources/pom.xml"])
cmd("remove backup pom.xml", "rm streams/quickstart/pom.xml.orig")
cmd("remove backup java pom.xml", "rm streams/quickstart/java/pom.xml.orig")
cmd("remove backup java pom.xml", "rm streams/quickstart/java/src/main/resources/archetype-resources/pom.xml.orig")
# Command in explicit list due to messages with spaces
cmd("Commiting version number updates", ["git", "commit", "-a", "-m", "Bump version to %s" % release_version])
# Command in explicit list due to messages with spaces
cmd("Tagging release candidate %s" % rc_tag, ["git", "tag", "-a", rc_tag, "-m", rc_tag])
rc_githash = cmd_output("git show-ref --hash " + rc_tag)
cmd("Switching back to your starting branch", "git checkout %s" % starting_branch)

# Note that we don't use tempfile here because mkdtemp causes problems with sftp and being able to determine the absolute path to a file.
# Instead we rely on a fixed path and if it
work_dir = os.path.join(REPO_HOME, ".release_work_dir")
if os.path.exists(work_dir):
    fail("A previous attempt at a release left dirty state in the work directory. Clean up %s before proceeding. (This attempt will try to cleanup, simply retrying may be sufficient now...)" % work_dir)
os.makedirs(work_dir)
print("Temporary build working director:", work_dir)
kafka_dir = os.path.join(work_dir, 'kafka')
streams_quickstart_dir = os.path.join(kafka_dir, 'streams/quickstart')
print("Streams quickstart dir", streams_quickstart_dir)
cmd("Creating staging area for release artifacts", "mkdir kafka-" + rc_tag, cwd=work_dir)
artifacts_dir = os.path.join(work_dir, "kafka-" + rc_tag)
cmd("Cloning clean copy of repo", "git clone %s kafka" % REPO_HOME, cwd=work_dir)
cmd("Checking out RC tag", "git checkout -b %s %s" % (release_version, rc_tag), cwd=kafka_dir)
current_year = datetime.datetime.now().year
cmd("Verifying the correct year in NOTICE", "grep %s NOTICE" % current_year, cwd=kafka_dir)

with open(os.path.join(artifacts_dir, "RELEASE_NOTES.html"), 'w') as f:
    print("Generating release notes")
    try:
        subprocess.check_call(["./release_notes.py", release_version], stdout=f)
    except subprocess.CalledProcessError as e:
        print_output(e.output)

        print("*************************************************")
        print("*** First command failure occurred here.      ***")
        print("*** Will now try to clean up working state.   ***")
        print("*************************************************")
        fail("")


params = { 'release_version': release_version,
           'rc_tag': rc_tag,
           'artifacts_dir': artifacts_dir
           }
cmd("Creating source archive", "git archive --format tar.gz --prefix kafka-%(release_version)s-src/ -o %(artifacts_dir)s/kafka-%(release_version)s-src.tgz %(rc_tag)s" % params)

cmd("Building artifacts", "gradle", cwd=kafka_dir, env=jdk7_env)
cmd("Building artifacts", "./gradlew clean releaseTarGzAll aggregatedJavadoc", cwd=kafka_dir, env=jdk7_env)
# we need extra cmd to build 2.12 with jdk8 specifically
cmd("Building artifacts for Scala 2.12", "./gradlew releaseTarGz -PscalaVersion=2.12", cwd=kafka_dir, env=jdk8_env)
cmd("Copying artifacts", "cp %s/core/build/distributions/* %s" % (kafka_dir, artifacts_dir), shell=True)
cmd("Copying artifacts", "cp -R %s/build/docs/javadoc %s" % (kafka_dir, artifacts_dir))

for filename in os.listdir(artifacts_dir):
    full_path = os.path.join(artifacts_dir, filename)
    if not os.path.isfile(full_path):
        continue
    # Commands in explicit list due to key_name possibly containing spaces
    cmd("Signing " + full_path, ["gpg", "--batch", "--passphrase-fd", "0", "-u", key_name, "--armor", "--output", full_path + ".asc", "--detach-sig", full_path], stdin=gpg_passphrase)
    cmd("Verifying " + full_path, ["gpg", "--verify", full_path + ".asc", full_path])
    # Note that for verification, we need to make sure only the filename is used with --print-md because the command line
    # argument for the file is included in the output and verification uses a simple diff that will break if an absolut path
    # is used.
    dir, fname = os.path.split(full_path)
    cmd("Generating MD5 for " + full_path, "gpg --print-md md5 %s > %s.md5" % (fname, fname), shell=True, cwd=dir)
    cmd("Generating SHA1 for " + full_path, "gpg --print-md sha1 %s > %s.sha1" % (fname, fname), shell=True, cwd=dir)
    cmd("Generating SHA512 for " + full_path, "gpg --print-md sha512 %s > %s.sha512" % (fname, fname), shell=True, cwd=dir)

cmd("Listing artifacts to be uploaded:", "ls -R %s" % artifacts_dir)
if not user_ok("Going to upload the artifacts in %s, listed above, to your Apache home directory. Ok (y/n)?): " % artifacts_dir):
    fail("Quitting")
sftp_mkdir("public_html")
kafka_output_dir = "kafka-" + rc_tag
sftp_mkdir(os.path.join("public_html", kafka_output_dir))
public_release_dir = os.path.join("public_html", kafka_output_dir)
# The sftp -r option doesn't seem to work as would be expected, at least with the version shipping on OS X. To work around this we process all the files and directories manually...
sftp_cmds = ""
for root, dirs, files in os.walk(artifacts_dir):
    assert root.startswith(artifacts_dir)

    for file in files:
        local_path = os.path.join(root, file)
        remote_path = os.path.join("public_html", kafka_output_dir, root[len(artifacts_dir)+1:], file)
        sftp_cmds += "\nput %s %s" % (local_path, remote_path)

    for dir in dirs:
        sftp_mkdir(os.path.join("public_html", kafka_output_dir, root[len(artifacts_dir)+1:], dir))

if sftp_cmds:
    cmd("Uploading artifacts in %s to your Apache home directory" % root, "sftp -b - %s@home.apache.org" % apache_id, stdin=sftp_cmds)

with open(os.path.expanduser("~/.gradle/gradle.properties")) as f:
    contents = f.read()
if not user_ok("Going to build and upload mvn artifacts based on these settings:\n" + contents + '\nOK (y/n)?: '):
    fail("Retry again later")
cmd("Building and uploading archives", "./gradlew uploadArchivesAll", cwd=kafka_dir, env=jdk7_env)
cmd("Building and uploading archives", "./gradlew uploadCoreArchives_2_12 -PscalaVersion=2.12", cwd=kafka_dir, env=jdk8_env)
cmd("Building and uploading archives", "mvn deploy", cwd=streams_quickstart_dir, env=jdk7_env)

release_notification_props = { 'release_version': release_version,
                               'rc': rc,
                               'rc_tag': rc_tag,
                               'rc_githash': rc_githash,
                               'dev_branch': dev_branch,
                               'docs_version': docs_version,
                               'apache_id': apache_id,
                               }

# TODO: Many of these suggested validation steps could be automated and would help pre-validate a lot of the stuff voters test
print("""
*******************************************************************************************************************************************************
Ok. We've built and staged everything for the %(rc_tag)s.

Now you should sanity check it before proceeding. All subsequent steps start making RC data public.

Some suggested steps:

 * Grab the source archive and make sure it compiles: http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz
 * Grab one of the binary distros and run the quickstarts against them: http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka_2.11-%(release_version)s.tgz
 * Extract and verify one of the site docs jars: http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka_2.11-%(release_version)s-site-docs.tgz
 * Build a sample against jars in the staging repo: (TODO: Can we get a temporary URL before "closing" the staged artifacts?)
 * Validate GPG signatures on at least one file:
      wget http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz &&
      wget http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.asc &&
      wget http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.md5 &&
      wget http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.sha1 &&
      wget http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.sha512 &&
      gpg --verify kafka-%(release_version)s-src.tgz.asc kafka-%(release_version)s-src.tgz &&
      gpg --print-md md5 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.md5 &&
      gpg --print-md sha1 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.sha1 &&
      gpg --print-md sha512 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.sha512 &&
      rm kafka-%(release_version)s-src.tgz* &&
      echo "OK" || echo "Failed"
 * Validate the javadocs look ok. They are at http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/javadoc/

*******************************************************************************************************************************************************
""" % release_notification_props)
if not user_ok("Have you sufficiently verified the release artifacts (y/n)?: "):
    fail("Ok, giving up")

print("Next, we need to get the Maven artifacts we published into the staging repository.")
# TODO: Can we get this closed via a REST API since we already need to collect credentials for this repo?
print("Go to https://repository.apache.org/#stagingRepositories and hit 'Close' for the new repository that was created by uploading artifacts.")
if not user_ok("Have you successfully deployed the artifacts (y/n)?: "):
    fail("Ok, giving up")
if not user_ok("Ok to push RC tag %s (y/n)?: " % rc_tag):
    fail("Ok, giving up")
cmd("Pushing RC tag", "git push %s %s" % (PUSH_REMOTE_NAME, rc_tag))

# Move back to starting branch and clean out the temporary release branch (e.g. 1.0.0) we used to generate everything
cmd("Resetting repository working state", "git reset --hard HEAD && git checkout %s" % starting_branch, shell=True)
cmd("Deleting git branches %s" % release_version, "git branch -D %s" % release_version, shell=True)


email_contents = """
To: dev@kafka.apache.org, users@kafka.apache.org, kafka-clients@googlegroups.com
Subject: [VOTE] %(release_version)s RC%(rc)s

Hello Kafka users, developers and client-developers,

This is the first candidate for release of Apache Kafka %(release_version)s.

<DESCRIPTION OF MAJOR CHANGES, INCLUDE INDICATION OF MAJOR/MINOR RELEASE>

Release notes for the %(release_version)s release:
http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/RELEASE_NOTES.html

*** Please download, test and vote by <VOTING DEADLINE, e.g. Monday, March 28, 9am PT>

Kafka's KEYS file containing PGP keys we use to sign the release:
http://kafka.apache.org/KEYS

* Release artifacts to be voted upon (source and binary):
http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/

* Maven artifacts to be voted upon:
https://repository.apache.org/content/groups/staging/

* Javadoc:
http://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/javadoc/

* Tag to be voted upon (off %(dev_branch)s branch) is the %(release_version)s tag:
https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=tag;h=%(rc_githash)s

* Documentation:
http://kafka.apache.org/%(docs_version)s/documentation.html

* Protocol:
http://kafka.apache.org/%(docs_version)s/protocol.html

* Successful Jenkins builds for the %(dev_branch)s branch:
Unit/integration tests: https://builds.apache.org/job/kafka-%(dev_branch)s-jdk7/<BUILD NUMBER>/
System tests: https://jenkins.confluent.io/job/system-test-kafka-%(dev_branch)s/<BUILD_NUMBER>/

/**************************************

Thanks,
<YOU>
""" % release_notification_props

print()
print()
print("*****************************************************************")
print()
print(email_contents)
print()
print("*****************************************************************")
print()
print("All artifacts should now be fully staged. Use the above template to send the announcement for the RC to the mailing list.")
print("IMPORTANT: Note that there are still some substitutions that need to be made in the template:")
print("  - Describe major changes in this release")
print("  - Deadline for voting, which should be at least 3 days after you send out the email")
print("  - Jenkins build numbers for successful unit & system test builds")
print("  - Fill in your name in the signature")
print("  - Finally, validate all the links before shipping!")
print("Note that all substitutions are annotated with <> around them.")
