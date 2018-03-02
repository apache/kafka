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

package org.apache.kafka.streams.kstream.internals;

/**
 * Used to Build a Topology and apply optimization rules as the graph of the topology
 * is traversed.
 */
public interface TopologyOptimizer {


    /**
     * Called to build and optimize the topology. During this process results stored in the graph
     * are applied to the topology.
     *
     * @param streamsTopologyGraph
     */
    void optimize(StreamsTopologyGraph streamsTopologyGraph);

}
