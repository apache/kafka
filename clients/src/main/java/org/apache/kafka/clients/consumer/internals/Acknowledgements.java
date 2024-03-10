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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;

import java.util.LinkedHashMap;
import java.util.Map;

public class Acknowledgements {
    private final Map<Long, AcknowledgeType> acknowledgements;

    public static Acknowledgements empty() {
        return new Acknowledgements(new LinkedHashMap<>());
    }

    private Acknowledgements(Map<Long, AcknowledgeType> acknowledgements) {
        this.acknowledgements = acknowledgements;
    }

    public void add(long offset,
                    AcknowledgeType type) {
        this.acknowledgements.put(offset, type);
    }

    public void merge(Acknowledgements other) {
        acknowledgements.putAll(other.acknowledgements);
    }
}
