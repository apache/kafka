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

Usage: release.py [subcommand]

release.py stage

  Builds and stages an RC for a release.

  The utility is interactive; you will be prompted for basic release information and guided through the process.

  This utility assumes you already have local a kafka git folder and that you
  have added remotes corresponding to both:
  (i) the github apache kafka mirror and
  (ii) the apache kafka git repo.

release.py stage-docs [kafka-site-path]

  Builds the documentation and stages it into an instance of the Kafka website repository.

  This is meant to automate the integration between the main Kafka website repository (https://github.com/apache/kafka-site)
  and the versioned documentation maintained in the main Kafka repository. This is useful both for local testing and
  development of docs (follow the instructions here: https://cwiki.apache.org/confluence/display/KAFKA/Setup+Kafka+Website+on+Local+Apache+Server)
  as well as for committers to deploy docs (run this script, then validate, commit, and push to kafka-site).

  With no arguments this script assumes you have the Kafka repository and kafka-site repository checked out side-by-side, but
  you can specify a full path to the kafka-site repository if this is not the case.

release.py release-email

  Generates the email content/template for sending release announcement email.

"""

from __future__ import print_function

import datetime
from getpass import getpass
import json
import os
import subprocess
import sys
import tempfile
import time
import re

PROJECT_NAME = "kafka"
CAPITALIZED_PROJECT_NAME = "kafka".upper()
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
# Location of the local git repository
REPO_HOME = os.environ.get("%s_HOME" % CAPITALIZED_PROJECT_NAME, SCRIPT_DIR)
# Remote name, which points to Github by default
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache-github")
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

def cmd(action, cmd_arg, *args, **kwargs):
    if isinstance(cmd_arg, basestring) and not kwargs.get("shell", False):
        cmd_arg = cmd_arg.split()
    allow_failure = kwargs.pop("allow_failure", False)
    num_retries = kwargs.pop("num_retries", 0)

    stdin_log = ""
    if "stdin" in kwargs and isinstance(kwargs["stdin"], basestring):
        stdin_log = "--> " + kwargs["stdin"]
        stdin = tempfile.TemporaryFile()
        stdin.write(kwargs["stdin"])
        stdin.seek(0)
        kwargs["stdin"] = stdin

    print(action, cmd_arg, stdin_log)
    try:
        output = subprocess.check_output(cmd_arg, *args, stderr=subprocess.STDOUT, **kwargs)
        print_output(output)
    except subprocess.CalledProcessError as e:
        print_output(e.output)

        if num_retries > 0:
            kwargs['num_retries'] = num_retries - 1
            kwargs['allow_failure'] = allow_failure
            print("Retrying... %d remaining retries" % (num_retries - 1))
            time.sleep(4. / (num_retries + 1)) # e.g., if retries=3, sleep for 1s, 1.3s, 2s
            return cmd(action, cmd_arg, *args, **kwargs)

        if allow_failure:
            return

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

def regexReplace(path, pattern, replacement):
    updated = []
    with open(path, 'r') as f:
        for line in f:
            updated.append(re.sub(pattern, replacement, line))

    with open(path, 'w') as f:
        for line in updated:
            f.write(line)

def user_ok(msg):
    ok = raw_input(msg)
    return ok.strip().lower() == 'y'

def sftp_mkdir(dir):
    basedir, dirname = os.path.split(dir)
    if not basedir:
       basedir = "."
    try:
       cmd_str  = """
cd %s
-mkdir %s
""" % (basedir, dirname)
       cmd("Creating '%s' in '%s' in your Apache home directory if it does not exist (errors are ok if the directory already exists)" % (dirname, basedir), "sftp -b - %s@home.apache.org" % apache_id, stdin=cmd_str, allow_failure=True, num_retries=3)
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

def load_prefs():
    """Load saved preferences"""
    prefs = {}
    if os.path.exists(PREFS_FILE):
        with open(PREFS_FILE, 'r') as prefs_fp:
            prefs = json.load(prefs_fp)
    return prefs

def save_prefs(prefs):
    """Save preferences"""
    print("Saving preferences to %s" % PREFS_FILE)
    with open(PREFS_FILE, 'w') as prefs_fp:
        prefs = json.dump(prefs, prefs_fp)

def get_jdk(prefs, version):
    """
    Get settings for the specified JDK version.
    """
    jdk_java_home = get_pref(prefs, 'jdk%d' % version, lambda: raw_input("Enter the path for JAVA_HOME for a JDK%d compiler (blank to use default JAVA_HOME): " % version))
    jdk_env = dict(os.environ) if jdk_java_home.strip() else None
    if jdk_env is not None: jdk_env['JAVA_HOME'] = jdk_java_home
    javaVersion = cmd_output("%s/bin/java -version" % jdk_java_home, env=jdk_env)
    if version == 8 and "1.8.0" not in javaVersion:
      fail("JDK 8 is required")
    elif "%d.0" % version not in javaVersion:
      fail("JDK %s is required" % version)
    return jdk_env

def get_version(repo=REPO_HOME):
    """
    Extracts the full version information as a str from gradle.properties
    """
    with open(os.path.join(repo, 'gradle.properties')) as fp:
        for line in fp:
            parts = line.split('=')
            if parts[0].strip() != 'version': continue
            return parts[1].strip()
    fail("Couldn't extract version from gradle.properties")

def docs_version(version):
    """
    Detects the major/minor version and converts it to the format used for docs on the website, e.g. gets 0.10.2.0-SNAPSHOT
    from gradle.properties and converts it to 0102
    """
    version_parts = version.strip().split('.')
    # 1.0+ will only have 3 version components as opposed to pre-1.0 that had 4
    major_minor = version_parts[0:3] if version_parts[0] == '0' else version_parts[0:2]
    return ''.join(major_minor)

def docs_release_version(version):
    """
    Detects the version from gradle.properties and converts it to a release version number that should be valid for the
    current release branch. For example, 0.10.2.0-SNAPSHOT would remain 0.10.2.0-SNAPSHOT (because no release has been
    made on that branch yet); 0.10.2.1-SNAPSHOT would be converted to 0.10.2.0 because 0.10.2.1 is still in development
    but 0.10.2.0 should have already been released. Regular version numbers (e.g. as encountered on a release branch)
    will remain the same.
    """
    version_parts = version.strip().split('.')
    if '-SNAPSHOT' in version_parts[-1]:
        bugfix = int(version_parts[-1].split('-')[0])
        if bugfix > 0:
            version_parts[-1] = str(bugfix - 1)
    return '.'.join(version_parts)

def command_stage_docs():
    kafka_site_repo_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(REPO_HOME, '..', 'kafka-site')
    if not os.path.exists(kafka_site_repo_path) or not os.path.exists(os.path.join(kafka_site_repo_path, 'powered-by.html')):
        sys.exit("%s doesn't exist or does not appear to be the kafka-site repository" % kafka_site_repo_path)

    prefs = load_prefs()
    jdk11_env = get_jdk(prefs, 11)
    save_prefs(prefs)

    version = get_version()
    # We explicitly override the version of the project that we normally get from gradle.properties since we want to be
    # able to run this from a release branch where we made some updates, but the build would show an incorrect SNAPSHOT
    # version due to already having bumped the bugfix version number.
    gradle_version_override = docs_release_version(version)

    cmd("Building docs", "./gradlew -Pversion=%s clean siteDocsTar aggregatedJavadoc" % gradle_version_override, cwd=REPO_HOME, env=jdk11_env)

    docs_tar = os.path.join(REPO_HOME, 'core', 'build', 'distributions', 'kafka_2.13-%s-site-docs.tgz' % gradle_version_override)

    versioned_docs_path = os.path.join(kafka_site_repo_path, docs_version(version))
    if not os.path.exists(versioned_docs_path):
        os.mkdir(versioned_docs_path, 0755)

    # The contents of the docs jar are site-docs/<docs dir>. We need to get rid of the site-docs prefix and dump everything
    # inside it into the docs version subdirectory in the kafka-site repo
    cmd('Extracting site-docs', 'tar xf %s --strip-components 1' % docs_tar, cwd=versioned_docs_path)

    javadocs_src_dir = os.path.join(REPO_HOME, 'build', 'docs', 'javadoc')

    cmd('Copying javadocs', 'cp -R %s %s' % (javadocs_src_dir, versioned_docs_path))

    sys.exit(0)

def validate_release_version_parts(version):
    try:
        version_parts = version.split('.')
        if len(version_parts) != 3:
            fail("Invalid release version, should have 3 version number components")
        # Validate each part is a number
        [int(x) for x in version_parts]
    except ValueError:
        fail("Invalid release version, should be a dotted version number")

def get_release_version_parts(version):
    validate_release_version_parts(version)
    return version.split('.')

def validate_release_num(version):
    tags = cmd_output('git tag').split()
    if version not in tags:
        fail("The specified version is not a valid release version number")
    validate_release_version_parts(version)

def command_release_announcement_email():
    tags = cmd_output('git tag').split()
    release_tag_pattern = re.compile('^[0-9]+\.[0-9]+\.[0-9]+$')
    release_tags = sorted([t for t in tags if re.match(release_tag_pattern, t)])
    release_version_num = release_tags[-1]
    if not user_ok("""Is the current release %s ? (y/n): """ % release_version_num):
        release_version_num = raw_input('What is the current release version:')
        validate_release_num(release_version_num)
    previous_release_version_num = release_tags[-2]
    if not user_ok("""Is the previous release %s ? (y/n): """ % previous_release_version_num):
        previous_release_version_num = raw_input('What is the previous release version:')
        validate_release_num(previous_release_version_num)
    if release_version_num < previous_release_version_num :
        fail("Current release version number can't be less than previous release version number")
    number_of_contributors = int(subprocess.check_output('git shortlog -sn --no-merges %s..%s | wc -l' % (previous_release_version_num, release_version_num) , shell=True))
    contributors = subprocess.check_output("git shortlog -sn --no-merges %s..%s | cut -f2 | sort --ignore-case" % (previous_release_version_num, release_version_num), shell=True)
    release_announcement_data = {
        'number_of_contributors': number_of_contributors,
        'contributors': ', '.join(str(x) for x in filter(None, contributors.split('\n'))),
        'release_version': release_version_num
    }

    release_announcement_email = """
To: announce@apache.org, dev@kafka.apache.org, users@kafka.apache.org, kafka-clients@googlegroups.com
Subject: [ANNOUNCE] Apache Kafka %(release_version)s

The Apache Kafka community is pleased to announce the release for Apache Kafka %(release_version)s

<DETAILS OF THE CHANGES>

All of the changes in this release can be found in the release notes:
https://www.apache.org/dist/kafka/%(release_version)s/RELEASE_NOTES.html


You can download the source and binary release (Scala <VERSIONS>) from:
https://kafka.apache.org/downloads#%(release_version)s

---------------------------------------------------------------------------------------------------


Apache Kafka is a distributed streaming platform with four core APIs:


** The Producer API allows an application to publish a stream records to
one or more Kafka topics.

** The Consumer API allows an application to subscribe to one or more
topics and process the stream of records produced to them.

** The Streams API allows an application to act as a stream processor,
consuming an input stream from one or more topics and producing an
output stream to one or more output topics, effectively transforming the
input streams to output streams.

** The Connector API allows building and running reusable producers or
consumers that connect Kafka topics to existing applications or data
systems. For example, a connector to a relational database might
capture every change to a table.


With these APIs, Kafka can be used for two broad classes of application:

** Building real-time streaming data pipelines that reliably get data
between systems or applications.

** Building real-time streaming applications that transform or react
to the streams of data.


Apache Kafka is in use at large and small companies worldwide, including
Capital One, Goldman Sachs, ING, LinkedIn, Netflix, Pinterest, Rabobank,
Target, The New York Times, Uber, Yelp, and Zalando, among others.

A big thank you for the following %(number_of_contributors)d contributors to this release!

%(contributors)s

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at
https://kafka.apache.org/

Thank you!


Regards,

<YOU>""" % release_announcement_data

    print()
    print("*****************************************************************")
    print()
    print(release_announcement_email)
    print()
    print("*****************************************************************")
    print()
    print("Use the above template to send the announcement for the release to the mailing list.")
    print("IMPORTANT: Note that there are still some substitutions that need to be made in the template:")
    print("  - Describe major changes in this release")
    print("  - Scala versions")
    print("  - Fill in your name in the signature")
    print("  - You will need to use your apache email address to send out the email (otherwise, it won't be delivered to announce@apache.org)")
    print("  - Finally, validate all the links before shipping!")
    print("Note that all substitutions are annotated with <> around them.")
    sys.exit(0)


# Dispatch to subcommand
subcommand = sys.argv[1] if len(sys.argv) > 1 else None
if subcommand == 'stage-docs':
    command_stage_docs()
elif subcommand == 'release-email':
    command_release_announcement_email()
elif not (subcommand is None or subcommand == 'stage'):
    fail("Unknown subcommand: %s" % subcommand)
# else -> default subcommand stage


## Default 'stage' subcommand implementation isn't isolated to its own function yet for historical reasons

prefs = load_prefs()

if not user_ok("""Requirements:
1. Updated docs to reference the new release version where appropriate.
2. JDK8 and JDK11 compilers and libraries
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
            <passphrase>your-gpg-passphrase</passphrase>
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
release_version_parts = get_release_version_parts(release_version)

rc = raw_input("Release candidate number: ")

dev_branch = '.'.join(release_version_parts[:2])
docs_release_version = docs_version(release_version)

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

jdk8_env = get_jdk(prefs, 8)
jdk11_env = get_jdk(prefs, 11)

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

save_prefs(prefs)

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
print("updating streams quickstart pom")
regexReplace("streams/quickstart/pom.xml", "-SNAPSHOT", "")
print("updating streams quickstart java pom")
regexReplace("streams/quickstart/java/pom.xml", "-SNAPSHOT", "")
print("updating streams quickstart archetype pom")
regexReplace("streams/quickstart/java/src/main/resources/archetype-resources/pom.xml", "-SNAPSHOT", "")
print("updating ducktape version.py")
regexReplace("./tests/kafkatest/version.py", "^DEV_VERSION =.*",
    "DEV_VERSION = KafkaVersion(\"%s-SNAPSHOT\")" % release_version)
# Command in explicit list due to messages with spaces
cmd("Committing version number updates", ["git", "commit", "-a", "-m", "Bump version to %s" % release_version])
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
artifact_name = "kafka-" + rc_tag
cmd("Creating staging area for release artifacts", "mkdir " + artifact_name, cwd=work_dir)
artifacts_dir = os.path.join(work_dir, artifact_name)
cmd("Cloning clean copy of repo", "git clone %s kafka" % REPO_HOME, cwd=work_dir)
cmd("Checking out RC tag", "git checkout -b %s %s" % (release_version, rc_tag), cwd=kafka_dir)
current_year = datetime.datetime.now().year
cmd("Verifying the correct year in NOTICE", "grep %s NOTICE" % current_year, cwd=kafka_dir)

with open(os.path.join(artifacts_dir, "RELEASE_NOTES.html"), 'w') as f:
    print("Generating release notes")
    try:
        subprocess.check_call([sys.executable, "./release_notes.py", release_version], stdout=f)
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

cmd("Building artifacts", "./gradlew clean && ./gradlewAll releaseTarGz", cwd=kafka_dir, env=jdk8_env, shell=True)
cmd("Copying artifacts", "cp %s/core/build/distributions/* %s" % (kafka_dir, artifacts_dir), shell=True)
cmd("Building docs", "./gradlew clean aggregatedJavadoc", cwd=kafka_dir, env=jdk11_env)
cmd("Copying docs", "cp -R %s/build/docs/javadoc %s" % (kafka_dir, artifacts_dir))

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

cmd("Zipping artifacts", "tar -czf %s.tar.gz %s" % (artifact_name, artifact_name), cwd=work_dir)
cmd("Uploading artifacts in %s to your Apache home directory" % artifacts_dir, "rsync %s.tar.gz %s@home.apache.org:%s.tar.gz" % (artifact_name, apache_id, artifact_name), cwd=work_dir)
cmd("Extracting artifacts in Apache public_html directory",
    ["ssh",
     "%s@home.apache.org" % (apache_id),
     "mkdir -p public_html && rm -rf public_html/%s && mv %s.tar.gz public_html/ && cd public_html/ && tar -xf %s.tar.gz && rm %s.tar.gz" % (artifact_name, artifact_name, artifact_name, artifact_name)
     ])

with open(os.path.expanduser("~/.gradle/gradle.properties")) as f:
    contents = f.read()
if not user_ok("Going to build and upload mvn artifacts based on these settings:\n" + contents + '\nOK (y/n)?: '):
    fail("Retry again later")
cmd("Building and uploading archives", "./gradlewAll publish", cwd=kafka_dir, env=jdk8_env, shell=True)
cmd("Building and uploading archives", "mvn deploy -Pgpg-signing", cwd=streams_quickstart_dir, env=jdk8_env, shell=True)

release_notification_props = { 'release_version': release_version,
                               'rc': rc,
                               'rc_tag': rc_tag,
                               'rc_githash': rc_githash,
                               'dev_branch': dev_branch,
                               'docs_version': docs_release_version,
                               'apache_id': apache_id,
                               }

# TODO: Many of these suggested validation steps could be automated and would help pre-validate a lot of the stuff voters test
print("""
*******************************************************************************************************************************************************
Ok. We've built and staged everything for the %(rc_tag)s.

Now you should sanity check it before proceeding. All subsequent steps start making RC data public.

Some suggested steps:

 * Grab the source archive and make sure it compiles: https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz
 * Grab one of the binary distros and run the quickstarts against them: https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka_2.13-%(release_version)s.tgz
 * Extract and verify one of the site docs jars: https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka_2.13-%(release_version)s-site-docs.tgz
 * Build a sample against jars in the staging repo: (TODO: Can we get a temporary URL before "closing" the staged artifacts?)
 * Validate GPG signatures on at least one file:
      wget https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz &&
      wget https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.asc &&
      wget https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.md5 &&
      wget https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.sha1 &&
      wget https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/kafka-%(release_version)s-src.tgz.sha512 &&
      gpg --verify kafka-%(release_version)s-src.tgz.asc kafka-%(release_version)s-src.tgz &&
      gpg --print-md md5 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.md5 &&
      gpg --print-md sha1 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.sha1 &&
      gpg --print-md sha512 kafka-%(release_version)s-src.tgz | diff - kafka-%(release_version)s-src.tgz.sha512 &&
      rm kafka-%(release_version)s-src.tgz* &&
      echo "OK" || echo "Failed"
 * Validate the javadocs look ok. They are at https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/javadoc/

*******************************************************************************************************************************************************
""" % release_notification_props)
if not user_ok("Have you sufficiently verified the release artifacts (y/n)?: "):
    fail("Ok, giving up")

print("Next, we need to get the Maven artifacts we published into the staging repository.")
# TODO: Can we get this closed via a REST API since we already need to collect credentials for this repo?
print("Go to https://repository.apache.org/#stagingRepositories and hit 'Close' for the new repository that was created by uploading artifacts.")
print("If this is not the first RC, you need to 'Drop' the previous artifacts.")
print("Confirm the correct artifacts are visible at https://repository.apache.org/content/groups/staging/org/apache/kafka/")
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
https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/RELEASE_NOTES.html

*** Please download, test and vote by <VOTING DEADLINE, e.g. Monday, March 28, 9am PT>

Kafka's KEYS file containing PGP keys we use to sign the release:
https://kafka.apache.org/KEYS

* Release artifacts to be voted upon (source and binary):
https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/

* Maven artifacts to be voted upon:
https://repository.apache.org/content/groups/staging/org/apache/kafka/

* Javadoc:
https://home.apache.org/~%(apache_id)s/kafka-%(rc_tag)s/javadoc/

* Tag to be voted upon (off %(dev_branch)s branch) is the %(release_version)s tag:
https://github.com/apache/kafka/releases/tag/%(rc_tag)s

* Documentation:
https://kafka.apache.org/%(docs_version)s/documentation.html

* Protocol:
https://kafka.apache.org/%(docs_version)s/protocol.html

* Successful Jenkins builds for the %(dev_branch)s branch:
Unit/integration tests: https://ci-builds.apache.org/job/Kafka/job/kafka/job/%(dev_branch)s/<BUILD NUMBER>/
System tests: https://jenkins.confluent.io/job/system-test-kafka/job/%(dev_branch)s/<BUILD_NUMBER>/

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
