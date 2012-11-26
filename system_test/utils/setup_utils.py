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
# setup_utils.py
# - This module provides some basic helper functions.
# =================================================================

import logging
import kafka_system_test_utils
import sys

class SetupUtils(object):

    # dict to pass user-defined attributes to logger argument: "extra"
    # to use: just update "thisClassName" to the appropriate value
    thisClassName = '(ReplicaBasicTest)'
    d = {'name_of_class': thisClassName}

    logger     = logging.getLogger("namedLogger")
    anonLogger = logging.getLogger("anonymousLogger")

    def __init__(self):
        d = {'name_of_class': self.__class__.__name__}
        self.logger.debug("#### constructor inside SetupUtils", extra=self.d)

    def log_message(self, message):
        print
        self.anonLogger.info("======================================================")
        self.anonLogger.info(message)
        self.anonLogger.info("======================================================")

