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

from mock import Mock, patch

from kafkatest.services.connect import ConnectServiceBase


class CheckConnectServiceBase(object):
    def check_append_module_to_classpath_ignores_javadoc_jar(self):
        classpath = self._append_module_to_classpath([
            "connect-file-4.0.0-SNAPSHOT-javadoc.jar",
            "connect-file-4.0.0-SNAPSHOT.jar",
        ])

        assert classpath == "export CLASSPATH=${CLASSPATH}:/opt/kafka-dev/connect/file/build/libs/connect-file-4.0.0-SNAPSHOT.jar; "

    def check_append_module_to_classpath_returns_empty_for_javadoc_only(self):
        classpath = self._append_module_to_classpath([
            "connect-file-4.0.0-SNAPSHOT-javadoc.jar",
        ])

        assert classpath == ""

    @staticmethod
    def _append_module_to_classpath(jar_files):
        service = Mock()
        service.path.home.return_value = "/opt/kafka-dev"

        with patch("kafkatest.services.connect.os.getcwd", return_value="/workspace"), \
                patch("kafkatest.services.connect.os.walk", return_value=[("", [], jar_files)]):
            return ConnectServiceBase.append_module_to_classpath(service, "file")
