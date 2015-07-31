/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.sink;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * Context passed to SinkTasks, allowing them to access utilities in the copycat runtime.
 */
public abstract class SinkTaskContext {
    private Map<TopicPartition, Long> offsets;

    public SinkTaskContext() {
        offsets = new HashMap<>();
    }

    public void resetOffset(Map<TopicPartition, Long> offsets) {
        this.offsets = offsets;
    }

    public Map<TopicPartition, Long> getOffsets() {
        return offsets;
    }
}
