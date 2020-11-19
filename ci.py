#!/usr/bin/python

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

import os
import logging
import re
import subprocess
import sys

from confluent.ci.scripts.ci_utils import run_cmd, regex_replace, replace
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)


class CI:

    def __init__(self, new_version, repo_path):
        """Initialize class variables"""
        # List of all the files that were modified by this script so the parent
        # script that runs this update can commit them.
        self.updated_files = []
        # The new version
        self.new_version = new_version
        # The path the root of the repo so we can use full absolute paths
        self.repo_path = repo_path

    def run_update(self):
        """Update all the files with the new version"""
        log.info("Running additional version updates for kafka")
        self.update_kafkatest()
        self.update_quickstart()
        log.info("Finished all kafka additional version updates.")

    def update_kafkatest(self):
        """Update kafka test python scripts"""
        log.info("Updating kafkatest init script.")
        init_file = os.path.join(self.repo_path, "tests/kafkatest/__init__.py")
        # Determine if this is a ccs or ce kafka version.
        kafka_qualifier = self.new_version.split("-")[2]
        dev_version = "{}.dev0".format(self.new_version.split("-{}".format(kafka_qualifier))[0])
        replace(init_file, "__version__", "__version__ = '{}'".format(dev_version))
        self.updated_files.append(init_file)
        log.info("Updating ducktape version.py")
        ducktape_version_file = os.path.join(self.repo_path, "tests/kafkatest/version.py")
        # The version in this file does not contain the qualifier
        ducktape_version = self.new_version.split("-{}".format(kafka_qualifier))[0]
        regex_replace(ducktape_version_file,
            "^DEV_VERSION = KafkaVersion.*",
            "DEV_VERSION = KafkaVersion(\"{}\")".format(ducktape_version))
        self.updated_files.append(ducktape_version_file)

    def update_quickstart(self):
        """Uodate the streams quick start pom files."""
        log.info("Updating streams quickstart pom files.")
        quickstart_pom = os.path.join(self.repo_path, "streams/quickstart/pom.xml")
        self.update_project_versions(quickstart_pom, self.new_version)
        self.updated_files.append(quickstart_pom)
        # Do not need to explicitly update this file because the above command updates all pom files in the project.
        # Just need to add it to the list of modified files.
        quickstart_java_pom = os.path.join(self.repo_path, "streams/quickstart/java/pom.xml")
        self.updated_files.append(quickstart_java_pom)
        # The maven plugin has fails to process this pom file because it is an archetype style, so have to use regex.
        log.info("Updating streams quickstart archetype pom")
        archetype_resources_pom = os.path.join(self.repo_path,
                                               "streams/quickstart/java/src/main/resources/archetype-resources/pom.xml")
        regex_replace(archetype_resources_pom,
            "<kafka\.version>.*</kafka\.version>",
            "<kafka.version>{}</kafka.version>".format(self.new_version))
        self.updated_files.append(archetype_resources_pom)

    def update_project_versions(self, pom_file, new_version):
        """Set the project version in the pom files to the new project version."""
        cmd = ["mvn", "--batch-mode", "versions:set",
            "-DnewVersion={} ".format(new_version),
            "-DallowSnapshots=false",
            "-DgenerateBackupPoms=false",
            "-DprocessAllModules=true",
            "-DprocessDependencies=false",
            "-DprocessPlugins=false",
            "-f",
            pom_file]
        log.info("Updating pom files with new project version.")
        _, success = run_cmd(cmd, self.repo_path)

        if not success:
            log.error("Failed to set the new version in the pom files.")
            sys.exit(1)

        log.info("Finished updating the pom files with new project version.")
