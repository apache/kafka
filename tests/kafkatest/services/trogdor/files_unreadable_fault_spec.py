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

from kafkatest.services.trogdor.task_spec import TaskSpec


class FilesUnreadableFaultSpec(TaskSpec):
    """
    The specification for a fault which makes files unreadable.
    """

    def __init__(self, start_ms, duration_ms, node_names, mount_path,
                 prefix, error_code):
        """
        Create a new FilesUnreadableFaultSpec.

        :param start_ms:        The start time, as described in task_spec.py
        :param duration_ms:     The duration in milliseconds.
        :param node_names:      The names of the node(s) to create the fault on.
        :param mount_path:      The mount path.
        :param prefix:          The prefix within the mount point to make unreadable.
        :param error_code:      The error code to use.
        """
        super(FilesUnreadableFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.FilesUnreadableFaultSpec"
        self.message["nodeNames"] = node_names
        self.message["mountPath"] = mount_path
        self.message["prefix"] = prefix
        self.message["errorCode"] = error_code

        self.kibosh_message = {}
        self.kibosh_message["type"] = "unreadable"
        self.kibosh_message["prefix"] = prefix
        self.kibosh_message["code"] = error_code
