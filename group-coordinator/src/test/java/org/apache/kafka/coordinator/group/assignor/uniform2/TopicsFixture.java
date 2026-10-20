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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;

/**
 * The topics of a test, described from a metadata image as the coordinator describes them. The
 * topics are named after their ids.
 *
 * <p>The topics are built once: the fixture cannot change afterwards.
 */
public final class TopicsFixture {
    private final MetadataImageBuilder image = new MetadataImageBuilder();
    private CoordinatorMetadataImage metadataImage;
    private SubscribedTopicDescriber subscribedTopicsDescriber;
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
     * Builds the metadata image and the describer of the topics.
     *
     * @return This fixture.
     */
    public TopicsFixture build() {
        checkNotBuilt();
        built = true;
        metadataImage = image.buildCoordinatorMetadataImage();
        subscribedTopicsDescriber = new SubscribedTopicDescriberImpl(metadataImage);
        return this;
    }

    /**
     * @return The metadata image of the topics.
     */
    public CoordinatorMetadataImage metadataImage() {
        checkBuilt();
        return metadataImage;
    }

    /**
     * @return The describer of the topics.
     */
    public SubscribedTopicDescriber subscribedTopicsDescriber() {
        checkBuilt();
        return subscribedTopicsDescriber;
    }

    private void checkBuilt() {
        if (!built) {
            throw new IllegalStateException("The topics are not built");
        }
    }

    private void checkNotBuilt() {
        if (built) {
            throw new IllegalStateException("The topics are already built");
        }
    }
}
