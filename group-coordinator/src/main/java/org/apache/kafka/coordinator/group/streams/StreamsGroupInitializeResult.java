/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.StreamsGroupInitializeResponseData;

public class StreamsGroupInitializeResult {

    private final StreamsGroupInitializeResponseData data;
    private final Map<String, CreatableTopic> creatableTopics;

    public StreamsGroupInitializeResult(StreamsGroupInitializeResponseData data, Map<String, CreatableTopic> creatableTopics) {
        this.data = data;
        this.creatableTopics = creatableTopics;
    }

    public StreamsGroupInitializeResult(StreamsGroupInitializeResponseData data) {
        this.data = data;
        this.creatableTopics = Collections.emptyMap();
    }

    public StreamsGroupInitializeResponseData data() {
        return data;
    }

    public Map<String, CreatableTopic> creatableTopics() {
        return creatableTopics;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupInitializeResult that = (StreamsGroupInitializeResult) o;
        return Objects.equals(data, that.data) && Objects.equals(creatableTopics,
            that.creatableTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, creatableTopics);
    }

    @Override
    public String toString() {
        return "StreamsGroupInitializeResult{" +
            "data=" + data +
            ", creatableTopics=" + creatableTopics +
            '}';
    }
}
