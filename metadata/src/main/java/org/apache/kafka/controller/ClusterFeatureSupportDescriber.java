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

package org.apache.kafka.controller;

import org.apache.kafka.metadata.VersionRange;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public interface ClusterFeatureSupportDescriber {
    Iterator<Entry<Integer, Map<String, VersionRange>>> brokerSupported();
    Iterator<Entry<Integer, Map<String, VersionRange>>> controllerSupported();

    /**
     * The IDs of the controllers which are currently members of the quorum.
     *
     * <p>This is separate from {@link #controllerSupported()} because a controller
     * registration can outlive the controller's membership in a dynamic quorum.
     *
     * @return the current quorum controller IDs, or an empty set when the caller
     *         should use its static quorum configuration
     */
    default Set<Integer> controllerIds() {
        return Set.of();
    }
}
