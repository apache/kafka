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
 */
package org.apache.kafka.common.internals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public final class TopicConstants {

    //avoid instantiation
    private TopicConstants() {
    }

    // TODO: we store both group metadata and offset data here despite the topic name being offsets only
    public static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
    public static final Collection<String> INTERNAL_TOPICS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(GROUP_METADATA_TOPIC_NAME)));
}
