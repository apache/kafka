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
Utility for creating release candidates and promoting release candidates to a final release.

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

import datetime
import os
import re
import subprocess
import sys
import time

from runtime import (
    append_fail_hook,
    cmd,
    confirm,
    confirm_or_fail,
    execute,
    fail,
    prompt,
    repo_dir,
)
import git
import gpg
import notes
import preferences
import svn
import templates
import textfiles

from svn import SVN_DEV_URL


def get_jdk(version):
    """
    Get settings for the specified JDK version.
    """
    msg = f"Enter the path for JAVA_HOME for a JDK{version} compiler (blank to use default JAVA_HOME): "
    key = f"jdk{version}"
    jdk_java_home = preferences.get(key, lambda: prompt(msg))
    jdk_env = dict(os.environ)
    if jdk_java_home.strip(): jdk_env["JAVA_HOME"] = jdk_java_home
    else: jdk_java_home = jdk_env["JAVA_HOME"]
    java_version = execute(f"{jdk_java_home}/bin/java -version", env=jdk_env)
    if (version == 8 and "1.8.0" not in java_version) or \
       (f"{version}.0" not in java_version and '"{version}"' not in java_version):
        preferences.unset(key)
        fail(f"JDK {version} is required")
    return jdk_env


def docs_version(version):
    """
    Detects the major/minor version and converts it to the format used for docs on the website, e.g. gets 0.10.2.0-SNAPSHOT
    from gradle.properties and converts it to 0102
    """
    version_parts = version.strip().split(".")
    # 1.0+ will only have 3 version components as opposed to pre-1.0 that had 4
    major_minor = version_parts[0:3] if version_parts[0] == "0" else version_parts[0:2]
    return ''.join(major_minor)


def detect_docs_release_version(version):
    """
    Detects the version from gradle.properties and converts it to a release version number that should be valid for the
    current release branch. For example, 0.10.2.0-SNAPSHOT would remain 0.10.2.0-SNAPSHOT (because no release has been
    made on that branch yet); 0.10.2.1-SNAPSHOT would be converted to 0.10.2.0 because 0.10.2.1 is still in development
    but 0.10.2.0 should have already been released. Regular version numbers (e.g. as encountered on a release branch)
    will remain the same.
    """
    version_parts = version.strip().split(".")
    if "-SNAPSHOT" in version_parts[-1]:
        bugfix = int(version_parts[-1].split("-")[0])
        if bugfix > 0:
            version_parts[-1] = str(bugfix - 1)
    return ".".join(version_parts)


def command_stage_docs():
    kafka_site_repo_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(repo_dir, "..", "kafka-site")
    if not os.path.exists(kafka_site_repo_path) or not os.path.exists(os.path.join(kafka_site_repo_path, "powered-by.html")):
        fail("{kafka_site_repo_path} doesn't exist or does not appear to be the kafka-site repository")

    jdk17_env = get_jdk(17)

    # We explicitly override the version of the project that we normally get from gradle.properties since we want to be
    # able to run this from a release branch where we made some updates, but the build would show an incorrect SNAPSHOT
    # version due to already having bumped the bugfix version number.
    gradle_version_override = detect_docs_release_version(project_version)

    cmd("Building docs", f"./gradlew -Pversion={gradle_version_override} clean siteDocsTar aggregatedJavadoc", cwd=repo_dir, env=jdk17_env)

    docs_tar = os.path.join(repo_dir, "core", "build", "distributions", f"kafka_2.13-{gradle_version_override}-site-docs.tgz")

    versioned_docs_path = os.path.join(kafka_site_repo_path, docs_version(project_version))
    if not os.path.exists(versioned_docs_path):
        os.mkdir(versioned_docs_path, 0o755)

    # The contents of the docs jar are site-docs/<docs dir>. We need to get rid of the site-docs prefix and dump everything
    # inside it into the docs version subdirectory in the kafka-site repo
    cmd("Extracting site-docs", f"tar xf {docs_tar} --strip-components 1", cwd=versioned_docs_path)

    javadocs_src_dir = os.path.join(repo_dir, "build", "docs", "javadoc")

    cmd("Copying javadocs", f"cp -R {javadocs_src_dir} {versioned_docs_path}")

    sys.exit(0)


def validate_release_version_parts(version):
    try:
        version_parts = version.split(".")
        if len(version_parts) != 3:
            fail("Invalid release version, should have 3 version number components")
        # Validate each part is a number
        [int(x) for x in version_parts]
    except ValueError:
        fail("Invalid release version, should be a dotted version number")


def get_release_version_parts(version):
    validate_release_version_parts(version)
    return version.split(".")


def validate_release_num(version):
    if version not in git.tags():
        fail("The specified version is not a valid release version number")
    validate_release_version_parts(version)


def command_release_announcement_email():
    release_tag_pattern = re.compile("^[0-9]+\\.[0-9]+\\.[0-9]+$")
    release_tags = sorted([t for t in git.tags() if re.match(release_tag_pattern, t)])
    release_version_num = release_tags[-1]
    if not confirm(f"Is the current release {release_version_num}?"):
        release_version_num = prompt("What is the current release version:")
        validate_release_num(release_version_num)
    previous_release_version_num = release_tags[-2]
    if not confirm(f"Is the previous release {previous_release_version_num}?"):
        previous_release_version_num = prompt("What is the previous release version:")
        validate_release_num(previous_release_version_num)
    if release_version_num < previous_release_version_num :
        fail("Current release version number can't be less than previous release version number")
    contributors = git.contributors(previous_release_version_num, release_version_num)
    release_announcement_email = templates.release_announcement_email(release_version_num, contributors)
    print(templates.release_announcement_email_instructions(release_announcement_email))
    sys.exit(0)


project_version = textfiles.prop(os.path.join(repo_dir, "gradle.properties"), 'version')
release_version = project_version.replace('-SNAPSHOT', '')
release_version_parts = get_release_version_parts(release_version)
dev_branch = '.'.join(release_version_parts[:2])
docs_release_version = docs_version(release_version)

# Dispatch to subcommand
subcommand = sys.argv[1] if len(sys.argv) > 1 else None
if subcommand == 'stage-docs':
    command_stage_docs()
elif subcommand == 'release-email':
    command_release_announcement_email()
elif not (subcommand is None or subcommand == 'stage'):
    fail(f"Unknown subcommand: {subcommand}")
# else -> default subcommand stage


## Default 'stage' subcommand implementation isn't isolated to its own function yet for historical reasons


def verify_gpg_key():
    if not gpg.key_exists(gpg_key_id):
        fail(f"GPG key {gpg_key_id} not found")
    if not gpg.valid_passphrase(gpg_key_id, gpg_passphrase):
        fail(f"GPG passprase not valid for key {gpg_key_id}")


preferences.once("verify_requirements", lambda: confirm_or_fail(templates.requirements_instructions(preferences.FILE, preferences.as_json())))
global_gradle_props = os.path.expanduser("~/.gradle/gradle.properties")
gpg_key_id = textfiles.prop(global_gradle_props, "signing.keyId")
gpg_passphrase = textfiles.prop(global_gradle_props, "signing.password")
gpg_key_pass_id = gpg.key_pass_id(gpg_key_id, gpg_passphrase)
preferences.once(f"verify_gpg_key_{gpg_key_pass_id}", verify_gpg_key)

apache_id = preferences.get('apache_id', lambda: prompt("Please enter your apache-id: "))
jdk8_env = get_jdk(8)
jdk17_env = get_jdk(17)


def verify_prerequeisites():
    print("Begin to check if you have met all the pre-requisites for the release process")
    def prereq(name, soft_check):
        try:
            result = soft_check()
            if result == False:
                fail(f"Pre-requisite not met: {name}")
            else:
                print(f"Pre-requisite met: {name}")
        except Exception as e:
            fail(f"Pre-requisite not met: {name}. Error: {e}")
    prereq('Apache Maven CLI (mvn) in PATH', lambda: "Apache Maven" in execute("mvn -v"))
    prereq("svn CLI in PATH", lambda: "svn" in execute("svn --version"))
    prereq("Verifying that you have no unstaged git changes", lambda: git.has_unstaged_changes())
    prereq("Verifying that you have no staged git changes", lambda: git.has_staged_changes())
    return True


preferences.once(f"verify_prerequeisites", verify_prerequeisites)

# Validate that the release doesn't already exist
git.fetch_tags()
if release_version in git.tags():
    fail(f"Version {release_version} has already been tagged and released.")

rc = prompt(f"Release version {release_version} candidate number: ")
if not rc:
    fail("Need a release candidate number.")
try:
    int(rc)
except ValueError:
    fail(f"Invalid release candidate number: {rc}")
rc_tag = release_version + '-rc' + rc

starting_branch = git.current_branch()
def delete_gitrefs():
    try:
        git.reset_hard_head()
        git.switch_branch(starting_branch)
        git.delete_branch(release_version)
        git.delete_tag(rc_tag)
    except subprocess.CalledProcessError:
        print("Failed when trying to clean up git references added by this script. You may need to clean up branches/tags yourself before retrying.")
        print("Expected git branch: " + release_version)
        print("Expected git tag: " + rc_tag)

git.create_branch(release_version, f"{git.push_remote_name}/{dev_branch}")
append_fail_hook("Delete gitrefs", delete_gitrefs)
print("Updating version numbers")
textfiles.replace(f"{repo_dir}/gradle.properties", "version", f"version={release_version}")
textfiles.replace(f"{repo_dir}/tests/kafkatest/__init__.py", "__version__", f"__version__ = '{release_version}'")
print("Updating streams quickstart pom")
textfiles.replace(f"{repo_dir}/streams/quickstart/pom.xml", "-SNAPSHOT", "", regex=True)
print("Updating streams quickstart java pom")
textfiles.replace(f"{repo_dir}/streams/quickstart/java/pom.xml", "-SNAPSHOT", "", regex=True)
print("Updating streams quickstart archetype pom")
textfiles.replace(f"{repo_dir}/streams/quickstart/java/src/main/resources/archetype-resources/pom.xml", "-SNAPSHOT", "", regex=True)
print("Updating ducktape version.py")
textfiles.replace(f"{repo_dir}/tests/kafkatest/version.py", "^DEV_VERSION =.*",
    f"DEV_VERSION = KafkaVersion(\"{release_version}-SNAPSHOT\")", regex=True)
print("Updating docs templateData.js")
textfiles.replace(f"{repo_dir}/docs/js/templateData.js", "-SNAPSHOT", "", regex=True)
git.commit(f"Bump version to {release_version}")
git.create_tag(rc_tag)
git.switch_branch(starting_branch)

# Note that we don't use tempfile here because mkdtemp causes problems with being able to determine the absolute path to a file.
# Instead we rely on a fixed path
work_dir = os.path.join(repo_dir, ".release_work_dir")
clean_up_work_dir = lambda: cmd("Cleaning up work directory", f"rm -rf {work_dir}")
if os.path.exists(work_dir):
    clean_up_work_dir()
os.makedirs(work_dir)
append_fail_hook("Clean up work dir", clean_up_work_dir)
print("Temporary build working directory:", work_dir)
kafka_dir = os.path.join(work_dir, 'kafka')
artifact_name = "kafka-" + rc_tag
cmd("Creating staging area for release artifacts", "mkdir " + artifact_name, cwd=work_dir)
artifacts_dir = os.path.join(work_dir, artifact_name)
git.clone(repo_dir, 'kafka', cwd=work_dir)
git.create_branch(release_version, rc_tag, cwd=kafka_dir)
current_year = datetime.datetime.now().year
cmd("Verifying the correct year in NOTICE", f"grep {current_year} NOTICE", cwd=kafka_dir)
svn.checkout_svn_dev(work_dir)

print("Generating release notes")
try:
    html = notes.generate(release_version)
    release_notes_path = os.path.join(artifacts_dir, "RELEASE_NOTES.html")
    textfiles.write(release_notes_path, html)
except Exception as e:
    fail(f"Failed to generate release notes: {e}")


git.targz(rc_tag, f"kafka-{release_version}-src/", f"{artifacts_dir}/kafka-{release_version}-src.tgz")
cmd("Building artifacts", "./gradlew clean && ./gradlew releaseTarGz -PscalaVersion=2.13", cwd=kafka_dir, env=jdk8_env, shell=True)
cmd("Copying artifacts", f"cp {kafka_dir}/core/build/distributions/* {artifacts_dir}", shell=True)
cmd("Building docs", "./gradlew clean aggregatedJavadoc", cwd=kafka_dir, env=jdk17_env)
cmd("Copying docs", f"cp -R {kafka_dir}/build/docs/javadoc {artifacts_dir}")

for filename in os.listdir(artifacts_dir):
    full_path = os.path.join(artifacts_dir, filename)
    if not os.path.isfile(full_path):
        continue
    sig_full_path = full_path + ".asc"
    gpg.sign(gpg_key_id, gpg_passphrase, full_path, sig_full_path)
    gpg.verify(full_path, sig_full_path)
    # Note that for verification, we need to make sure only the filename is used with --print-md because the command line
    # argument for the file is included in the output and verification uses a simple diff that will break if an absolute path
    # is used.
    dir, fname = os.path.split(full_path)
    cmd(f"Generating MD5    for {full_path}", f"gpg --print-md md5    {fname} > {fname}.md5   ", shell=True, cwd=dir)
    cmd(f"Generating SHA1   for {full_path}", f"gpg --print-md sha1   {fname} > {fname}.sha1  ", shell=True, cwd=dir)
    cmd(f"Generating SHA512 for {full_path}", f"gpg --print-md sha512 {fname} > {fname}.sha512", shell=True, cwd=dir)

cmd("Listing artifacts to be uploaded:", f"ls -R {artifacts_dir}")
cmd("Zipping artifacts", f"tar -czf {artifact_name}.tar.gz {artifact_name}", cwd=work_dir)

confirm_or_fail(f"Going to check in artifacts to svn under {SVN_DEV_URL}/{rc_tag}. OK?")
svn.commit_artifacts(rc_tag, artifacts_dir, work_dir)

confirm_or_fail("Going to build and upload mvn artifacts based on these settings:\n" + textfiles.read(global_gradle_props) + '\nOK?')
cmd("Building and uploading archives", "./gradlew publish -PscalaVersion=2.13", cwd=kafka_dir, env=jdk8_env, shell=True)
cmd("Building and uploading archives", "mvn deploy -Pgpg-signing", cwd=os.path.join(kafka_dir, "streams/quickstart"), env=jdk8_env, shell=True)

# TODO: Many of these suggested validation steps could be automated
# and would help pre-validate a lot of the stuff voters test
print(templates.sanity_check_instructions(release_version, rc_tag, apache_id))
confirm_or_fail("Have you sufficiently verified the release artifacts?")

# TODO: Can we close the staging repository via a REST API since we
# already need to collect credentials for this repo?
print(templates.deploy_instructions())
confirm_or_fail("Have you successfully deployed the artifacts?")
confirm_or_fail(f"Ok to push RC tag {rc_tag}?")
git.push_tag(rc_tag)

# Move back to starting branch and clean out the temporary release branch (e.g. 1.0.0) we used to generate everything
git.reset_hard_head()
git.switch_branch(starting_branch)
git.delete_branch(release_version)

rc_vote_email_text = templates.rc_vote_email_text(release_version, rc, rc_tag, dev_branch, docs_release_version, apache_id)
print(templates.rc_email_instructions(rc_vote_email_text))

