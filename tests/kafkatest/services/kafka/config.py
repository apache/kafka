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

import config_property


class KafkaConfig(dict):
    """A dictionary-like container class which allows for definition of overridable default values,
    which is also capable of "rendering" itself as a useable server.properties file.
    """

    DEFAULTS = {
        config_property.PORT: 9092,
        config_property.SOCKET_RECEIVE_BUFFER_BYTES: 65536,
        config_property.LOG_DIRS: "/mnt/kafka/kafka-data-logs-1,/mnt/kafka/kafka-data-logs-2",
        config_property.ZOOKEEPER_CONNECTION_TIMEOUT_MS: 2000
    }

    def __init__(self, **kwargs):
        super(KafkaConfig, self).__init__(**kwargs)

        # Set defaults
        for key, val in self.DEFAULTS.items():
            if not self.has_key(key):
                self[key] = val

    def render(self):
        """Render self as a series of lines key=val\n, and do so in a consistent order. """
        keys = [k for k in self.keys()]
        keys.sort()

        s = ""
        for k in keys:
            s += "%s=%s\n" % (k, str(self[k]))
        return s

