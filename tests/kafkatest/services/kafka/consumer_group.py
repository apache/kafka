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


# These are the group protocols we support. Most tests that use the new group coordinator will
# (eventually) be upgraded to test both of these consumer groups.
classic_group_protocol = 'classic'
consumer_group_protocol = 'consumer'
all_group_protocols = [classic_group_protocol, consumer_group_protocol]

# These are the remote assignors used by the new group coordinator.
range_remote_assignor = 'range'
uniform_remote_assignor = 'uniform'
all_remote_assignors = [range_remote_assignor, uniform_remote_assignor]


def is_consumer_group_protocol_enabled(group_protocol):
    """Check if the KIP-848 consumer group protocol is enabled."""
    return group_protocol is not None and group_protocol.lower() == consumer_group_protocol


def maybe_set_group_protocol(group_protocol, config=None):
    """Maybe include the KIP-848 group.protocol configuration if it's not None."""
    if config is None:
        config = {}

    if group_protocol is not None:
        config["group.protocol"] = group_protocol

    return config
