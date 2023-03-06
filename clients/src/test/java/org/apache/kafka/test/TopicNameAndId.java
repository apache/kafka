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
package org.apache.kafka.test;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

// For TESTS only.
public class TopicNameAndId {
    private final String name;
    private final Uuid uuid;

    public TopicNameAndId(String name, Uuid uuid) {
        this.name = name;
        this.uuid = uuid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, uuid);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TopicNameAndId))
            return false;

        TopicNameAndId other = (TopicNameAndId) obj;
        return Objects.equals(name, other.name) && Objects.equals(uuid, other.uuid);
    }
}
