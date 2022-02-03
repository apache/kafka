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

from ducktape.mark.resource import cluster
from kafkatest.tests.kafka_test import KafkaTest


class MiniTest(KafkaTest):

    def __init__(self, test_context):
        super(MiniTest, self).__init__(test_context, 1, 1)

    @cluster(num_nodes=2)
    def test_whatever(self):
        for node in self.kafka.nodes:
            assert self.kafka.alive(node)

