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

from mock import Mock

from kafkatest.version import DEV_BRANCH, V_0_8_2_2, get_version


class CheckVersion(object):
    def check_get_version(self):
        """Check default and override behavior of get_version"""
        node = None
        assert get_version(node) == DEV_BRANCH

        node = Mock()
        node.version = None
        assert get_version(node) == DEV_BRANCH

        node = Mock()
        node.version = V_0_8_2_2
        assert get_version(node) == V_0_8_2_2