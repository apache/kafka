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

import json


class TaskSpec(object):
    """
    The base class for a task specification.

    MAX_DURATION_MS         The longest duration we should use for a task specification.
    """

    MAX_DURATION_MS=10000000

    def __init__(self, start_ms, duration_ms):
        """
        Create a new task specification.

        :param start_ms:        The target start time in milliseconds since the epoch.
        :param duration_ms:     The duration in milliseconds.
        """
        self.message = {
            'startMs': start_ms,
            'durationMs': duration_ms
        }

    @staticmethod
    def to_node_names(nodes):
        """
        Convert an array of nodes or node names to an array of node names.
        """
        node_names = []
        for obj in nodes:
            if isinstance(obj, str):
                node_names.append(obj)
            else:
                node_names.append(obj.name)
        return node_names

    def __str__(self):
        return json.dumps(self.message)
