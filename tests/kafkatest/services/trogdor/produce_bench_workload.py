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


from ducktape.services.service import Service
from kafkatest.services.trogdor.task_spec import TaskSpec


class ProduceBenchWorkloadSpec(TaskSpec):
    def __init__(self, start_ms, duration_ms, producer_node, bootstrap_servers,
                 target_messages_per_sec, max_messages, producer_conf,
                 total_topics, active_topics):
        super(ProduceBenchWorkloadSpec, self).__init__(start_ms, duration_ms)
        self.producer_node = producer_node
        self.bootstrap_servers = bootstrap_servers
        self.target_messages_per_sec = target_messages_per_sec
        self.max_messages = max_messages
        self.producer_conf = producer_conf
        self.total_topics = total_topics
        self.active_topics = active_topics

    def message(self):
        return {
            "class": "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
            "startMs": self.start_ms,
            "durationMs": self.duration_ms,
            "producerNode": self.producer_node,
            "bootstrapServers": self.bootstrap_servers,
            "targetMessagesPerSec": self.target_messages_per_sec,
            "maxMessages": self.max_messages,
            "producerConf": self.producer_conf,
            "totalTopics": self.total_topics,
            "activeTopics": self.active_topics,
        }


class ProduceBenchWorkloadService(Service):
    def __init__(self, context, kafka):
        Service.__init__(self, context, num_nodes=1)
        self.bootstrap_servers = kafka.bootstrap_servers(validate=False)
        self.producer_node = self.nodes[0].account.hostname

    def free(self):
        Service.free(self)

    def wait_node(self, node, timeout_sec=None):
        pass

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        pass
