# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest.services.transactional_message_copier import TransactionalMessageCopier

from ducktape.utils.util import wait_until

def create_and_start_message_copier(test_context, kafka, consumer_group, input_topic, input_partition,
                                    output_topic, transaction_size, transaction_timeout, transactional_id, use_group_metadata):
    message_copier = TransactionalMessageCopier(
        context=test_context,
        num_nodes=1,
        kafka=kafka,
        transactional_id=transactional_id,
        consumer_group=consumer_group,
        input_topic=input_topic,
        input_partition=input_partition,
        output_topic=output_topic,
        max_messages=-1,
        transaction_size=transaction_size,
        transaction_timeout=transaction_timeout,
        use_group_metadata=use_group_metadata
    )
    message_copier.start()
    wait_until(lambda: message_copier.alive(message_copier.nodes[0]),
               timeout_sec=10,
               err_msg="Message copier failed to start after 10 s")
    return message_copier

def create_and_start_copiers(test_context, kafka, consumer_group, input_topic, output_topic, transaction_size,
                             transaction_timeout, num_copiers, use_group_metadata):
    copiers = []
    for i in range(0, num_copiers):
        copiers.append(create_and_start_message_copier(
            test_context=test_context,
            kafka=kafka,
            consumer_group=consumer_group,
            input_topic=input_topic,
            output_topic=output_topic,
            input_partition=i,
            transaction_size=transaction_size,
            transaction_timeout=transaction_timeout,
            transactional_id="copier-" + str(i),
            use_group_metadata=use_group_metadata
        ))
    return copiers
