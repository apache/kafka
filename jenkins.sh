#!/bin/bash
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

# This script is used for verifying changes in Jenkins. In order to provide faster feedback, the tasks are ordered so
# that faster tasks are executed in every module before slower tasks (if possible). For example, the unit tests for all
# the modules are executed before the integration tests.
./gradlew clean compileJava compileScala compileTestJava compileTestScala checkstyleMain checkstyleTest findbugsMain unitTest rat integrationTest --no-daemon -PxmlFindBugsReport=true -PtestLoggingEvents=started,passed,skipped,failed "$@"
