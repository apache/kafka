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

package org.apache.kafka.streams.kstream.internals.graph;

/**
 * Indicates a table processor node that behaves differently depending on whether the upstream
 * table is versioned or unversioned. Versioned semantics are disabled by default, and enabled
 * as part of {@code InternalStreamsBuilder#buildAndOptimizeTopology()} if an upstream table
 * is identified to be versioned.
 */
public interface VersionedSemanticsGraphNode {

    /**
     * @param useVersionedSemantics whether versioned semantics should be enabled
     */
    void enableVersionedSemantics(boolean useVersionedSemantics, String parentNodeName);

}
