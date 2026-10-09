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
package org.apache.kafka.coordinator.group.assignor.uniform2;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;

/**
 * The topics of a test, described from a metadata image as the coordinator describes them. The
 * topics are named after their ids.
 *
 * <p>The describer is built once: the fixture cannot change afterwards.
 */
public final class TopicsFixture {
    private final MetadataImageBuilder image = new MetadataImageBuilder();
    private boolean built;

    /**
     * Adds a topic.
     *
     * @param topicId    The topic id.
     * @param partitions The number of partitions.
     * @return This fixture.
     */
    public TopicsFixture withTopic(Uuid topicId, int partitions) {
        checkNotBuilt();
        image.addTopic(topicId, topicId.toString(), partitions);
        return this;
    }

    /**
     * @return The describer of the topics.
     */
    public SubscribedTopicDescriber build() {
        checkNotBuilt();
        built = true;
        return new SubscribedTopicDescriberImpl(image.buildCoordinatorMetadataImage());
    }

    private void checkNotBuilt() {
        if (built) {
            throw new IllegalStateException("The topics are already built");
        }
    }
}
