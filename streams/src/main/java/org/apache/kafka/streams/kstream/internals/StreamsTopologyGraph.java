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
 * The Graph containing the entire topology as
 * created by the user.  After going through an
 * optimizing phase the original topology may be changed
 */
interface StreamsTopologyGraph {

    /**
     * Add a StreamsGraphNode to the graph
     *
     * @param metadata the metadata node to add
     */
    <K, V> void addNode(StreamGraphNode<K, V> metadata);

    /**
     * Get the root node for this graph
     *
     * @return {@link StreamGraphNode}
     */
    StreamGraphNode getRoot();

}
