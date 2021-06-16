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
package org.apache.kafka.streams.integration;

public class NamedTopologyIntegrationTest {
    //TODO KAFKA-12648
    /**
     * Things to test in Pt. 2 -  Introduce TopologyMetadata to wrap InternalTopologyBuilders of named topologies:
     * 1. Verify changelog & repartition topics decorated with named topology
     * 2. Make sure app run and works with
     *         -multiple subtopologies
     *         -persistent state
     *         -multi-partition input & output topics
     *         -standbys
     *         -piped input and verified output records
     * 3. Is the task assignment balanced? Does KIP-441/warmup replica placement work as intended?
     */
}
