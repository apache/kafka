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
#!/usr/bin/env python

# =================================================================
# replication_utils.py
# - This module defines constant values specific to Kafka Replication
#   and also provides helper functions for Replication system test.
# =================================================================

import logging
import sys

class ReplicationUtils(object):

    thisClassName = '(ReplicationUtils)'
    d = {'name_of_class': thisClassName}

    logger     = logging.getLogger("namedLogger")
    anonLogger = logging.getLogger("anonymousLogger")

    def __init__(self, testClassInstance):
        super(ReplicationUtils, self).__init__()
        self.logger.debug("#### constructor inside ReplicationUtils", extra=self.d)

        # leader attributes
        self.isLeaderLogPattern             = "Completed the leader state transition"
        self.brokerShutDownCompletedPattern = "shut down completed"

        self.leaderAttributesDict = {}

        self.leaderAttributesDict["BROKER_SHUT_DOWN_COMPLETED_MSG"] = \
            self.brokerShutDownCompletedPattern

        self.leaderAttributesDict["REGX_BROKER_SHUT_DOWN_COMPLETED_PATTERN"] = \
            "\[(.*?)\] .* \[Kafka Server (.*?)\], " + \
            self.brokerShutDownCompletedPattern

        self.leaderAttributesDict["LEADER_ELECTION_COMPLETED_MSG"] = \
            self.isLeaderLogPattern

        self.leaderAttributesDict["REGX_LEADER_ELECTION_PATTERN"]  = \
            "\[(.*?)\] .* Broker (.*?): " + \
            self.leaderAttributesDict["LEADER_ELECTION_COMPLETED_MSG"] + \
            " for topic (.*?) partition (.*?) \(.*"

        # Controller attributes
        self.isControllerLogPattern    = "Controller startup complete"
        self.controllerAttributesDict  = {}
        self.controllerAttributesDict["CONTROLLER_STARTUP_COMPLETE_MSG"] = self.isControllerLogPattern
        self.controllerAttributesDict["REGX_CONTROLLER_STARTUP_PATTERN"] = "\[(.*?)\] .* \[Controller (.*?)\]: " + \
            self.controllerAttributesDict["CONTROLLER_STARTUP_COMPLETE_MSG"]

        # Data Loss Percentage Threshold in Ack = 1 cases
        self.ackOneDataLossThresholdPercent = 5.0

