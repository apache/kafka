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
package org.apache.kafka.coordinator.group.common;

import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.TopicsImage;

public class TopicAndClusterMetadata {
    private final TopicsImage topicsImage;
    private final ClusterImage clusterImage;

    public TopicAndClusterMetadata(TopicsImage topicsImage, ClusterImage clusterImage) {
        this.topicsImage = topicsImage;
        this.clusterImage = clusterImage;
    }

    /**
     * @return Topic metadata image.
     */
    public TopicsImage topicsImage() {
        return topicsImage;
    }

    /**
     * @return Cluster metadata image.
     */
    public ClusterImage clusterImage() {
        return clusterImage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        TopicAndClusterMetadata that = (TopicAndClusterMetadata) o;
        return topicsImage.equals(that.topicsImage) &&
            clusterImage.equals(that.clusterImage);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicsImage.hashCode();
        result = prime * result + clusterImage.hashCode();
        return result;
    }

    @Override
    public String toString() {
      return topicsImage() + "-" + clusterImage();
    }
}
