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
package org.apache.kafka.coordinator.group;

/**
 * An offset is considered expired based on different factors, such as the state of the group
 * and/or the GroupMetadata record version (for classic groups). This class is used to check
 * how offsets for the group should be expired.
 */
public interface OffsetExpirationCondition {

    /**
     * Given an offset metadata and offsets retention, return whether the offset is expired or not.
     *
     * @param offset               The offset metadata.
     * @param currentTimestampMs   The current timestamp.
     * @param offsetsRetentionMs   The offset retention.
     *
     * @return Whether the offset is considered expired or not.
     */
    boolean isOffsetExpired(OffsetAndMetadata offset, long currentTimestampMs, long offsetsRetentionMs);
}
