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
Text templates for long messages with instructions for the user.
We keep these in this separate file to avoid cluttering the script.
"""


def requirements_instructions(prefs_file, prefs):
    return f"""
Requirements:
1. Updated docs to reference the new release version where appropriate.
2. JDK8 and JDK17 compilers and libraries
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

Some of these may be used from these previous settings loaded from {prefs_file}:
{prefs}

Do you have all of of these setup?"""


def release_announcement_email(release_version, contributors):
    contributors_str = ", ".join(contributors)
    num_contributors = len(contributors)
    return f"""
To: announce@apache.org, dev@kafka.apache.org, users@kafka.apache.org, kafka-clients@googlegroups.com
Subject: [ANNOUNCE] Apache Kafka {release_version}

The Apache Kafka community is pleased to announce the release for Apache Kafka {release_version}

<DETAILS OF THE CHANGES>

All of the changes in this release can be found in the release notes:
https://www.apache.org/dist/kafka/{release_version}/RELEASE_NOTES.html


An overview of the release can be found in our announcement blog post:
https://kafka.apache.org/blog

You can download the source and binary release (Scala <VERSIONS>) from:
https://kafka.apache.org/downloads#{release_version}

---------------------------------------------------------------------------------------------------


Apache Kafka is a distributed streaming platform with four core APIs:


** The Producer API allows an application to publish a stream of records to
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

A big thank you for the following {num_contributors} contributors to this release! (Please report an unintended omission)

{contributors_str}

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at
https://kafka.apache.org/

Thank you!


Regards,

<YOU>
Release Manager for Apache Kafka {release_version}"""


def deploy_instructions():
    return """
Next, we need to get the Maven artifacts we published into the staging repository.
Go to https://repository.apache.org/#stagingRepositories and hit 'Close' for the new repository that was created by uploading artifacts.
There will be more than one repository entries created, please close all of them.
In some cases, you may get errors on some repositories while closing them, see KAFKA-15033.
If this is not the first RC, you need to 'Drop' the previous artifacts.
Confirm the correct artifacts are visible at https://repository.apache.org/content/groups/staging/org/apache/kafka/
"""


def sanity_check_instructions(release_version, rc_tag, apache_id):
    return f"""
*******************************************************************************************************************************************************
Ok. We've built and staged everything for the {rc_tag}.

Now you should sanity check it before proceeding. All subsequent steps start making RC data public.

Some suggested steps:

 * Grab the source archive and make sure it compiles: https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz
 * Grab one of the binary distros and run the quickstarts against them: https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka_2.13-{release_version}.tgz
 * Extract and verify one of the site docs jars: https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka_2.13-{release_version}-site-docs.tgz
 * Build a sample against jars in the staging repo: (TODO: Can we get a temporary URL before "closing" the staged artifacts?)
 * Validate GPG signatures on at least one file:
      wget https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz &&
      wget https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz.asc &&
      wget https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz.md5 &&
      wget https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz.sha1 &&
      wget https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/kafka-{release_version}-src.tgz.sha512 &&
      gpg --verify kafka-{release_version}-src.tgz.asc kafka-{release_version}-src.tgz &&
      gpg --print-md md5 kafka-{release_version}-src.tgz | diff - kafka-{release_version}-src.tgz.md5 &&
      gpg --print-md sha1 kafka-{release_version}-src.tgz | diff - kafka-{release_version}-src.tgz.sha1 &&
      gpg --print-md sha512 kafka-{release_version}-src.tgz | diff - kafka-{release_version}-src.tgz.sha512 &&
      rm kafka-{release_version}-src.tgz* &&
      echo "OK" || echo "Failed"
 * Validate the javadocs look ok. They are at https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/javadoc/

*******************************************************************************************************************************************************
"""


def rc_vote_email_text(release_version, rc, rc_tag, dev_branch, docs_version, apache_id):
    return f"""
To: dev@kafka.apache.org, users@kafka.apache.org, kafka-clients@googlegroups.com
Subject: [VOTE] {release_version} RC{rc}

Hello Kafka users, developers and client-developers,

This is the first candidate for release of Apache Kafka {release_version}.

<DESCRIPTION OF MAJOR CHANGES, INCLUDE INDICATION OF MAJOR/MINOR RELEASE>

Release notes for the {release_version} release:
https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/RELEASE_NOTES.html

*** Please download, test and vote by <VOTING DEADLINE, e.g. Monday, March 28, 9am PT>
<THE RELEASE POLICY (https://www.apache.org/legal/release-policy.html#release-approval) REQUIRES VOTES TO BE OPEN FOR MINIMUM OF 3 DAYS THEREFORE VOTING DEADLINE SHOULD BE AT LEAST 72 HOURS FROM THE TIME THIS EMAIL IS SENT.>

Kafka's KEYS file containing PGP keys we use to sign the release:
https://kafka.apache.org/KEYS

* Release artifacts to be voted upon (source and binary):
https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/

<USE docker/README.md FOR STEPS TO CREATE RELEASE CANDIDATE DOCKER IMAGE>
* Docker release artifacts to be voted upon:
apache/kafka:{rc_tag}
apache/kafka-native:{rc_tag}

* Maven artifacts to be voted upon:
https://repository.apache.org/content/groups/staging/org/apache/kafka/

* Javadoc:
https://dist.apache.org/repos/dist/dev/kafka/{rc_tag}/javadoc/

* Tag to be voted upon (off {dev_branch} branch) is the {release_version} tag:
https://github.com/apache/kafka/releases/tag/{rc_tag}

* Documentation:
https://kafka.apache.org/{docs_version}/documentation.html

* Protocol:
https://kafka.apache.org/{docs_version}/protocol.html

* Successful Jenkins builds for the {dev_branch} branch:
Unit/integration tests: https://ci-builds.apache.org/job/Kafka/job/kafka/job/{dev_branch}/<BUILD NUMBER>/
System tests: https://jenkins.confluent.io/job/system-test-kafka/job/{dev_branch}/<BUILD_NUMBER>/

<USE docker/README.md FOR STEPS TO RUN DOCKER BUILD TEST GITHUB ACTIONS>
* Successful Docker Image Github Actions Pipeline for {dev_branch} branch:
Docker Build Test Pipeline (JVM): https://github.com/apache/kafka/actions/runs/<RUN_NUMBER>
Docker Build Test Pipeline (Native): https://github.com/apache/kafka/actions/runs/<RUN_NUMBER>

/**************************************

Thanks,
<YOU>
"""


def rc_email_instructions(rc_email_text):
    return f"""
*****************************************************************

{rc_email_text}

*****************************************************************

All artifacts should now be fully staged. Use the above template to send the announcement for the RC to the mailing list.
IMPORTANT: Note that there are still some substitutions that need to be made in the template:
  - Describe major changes in this release
  - Deadline for voting, which should be at least 3 days after you send out the email
  - Jenkins build numbers for successful unit & system test builds
  - Fill in your name in the signature
  - Finally, validate all the links before shipping!
Note that all substitutions are annotated with <> around them.
"""


def cmd_failed():
    return """
*************************************************
*** First command failure occurred here.      ***
*** Will now try to clean up working state.   ***
*************************************************
"""


def release_announcement_email_instructions(release_announcement_email):
    return f"""
*****************************************************************

{release_announcement_email}

*****************************************************************

Use the above template to send the announcement for the release to the mailing list.
IMPORTANT: Note that there are still some substitutions that need to be made in the template:
  - Describe major changes in this release
  - Scala versions
  - Fill in your name in the signature
  - You will need to use your apache email address to send out the email (otherwise, it won't be delivered to announce@apache.org)
  - Finally, validate all the links before shipping!
Note that all substitutions are annotated with <> around them.
"""


