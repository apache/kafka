/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class MockPartitionAssignor extends AbstractPartitionAssignor {

    private Map<String, List<TopicPartition>> result = null;

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        if (result == null)
            throw new IllegalStateException("Call to assign with no result prepared");
        return result;
    }

    @Override
    public String name() {
        return "consumer-mock-assignor";
    }

    public void clear() {
        this.result = null;
    }

    public void prepare(Map<String, List<TopicPartition>> result) {
        this.result = result;
    }

}
