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

package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import java.util.Map;

/** Defines which topic configuration properties should be replicated. */
@InterfaceStability.Evolving
public interface ConfigPropertyFilter extends Configurable, AutoCloseable {

    /**
     * Specifies whether to replicate the given topic configuration.
     * Note that if a property has a default value on the source cluster,
     * {@link #shouldReplicateSourceDefault(String)} will also be called to
     * determine how that property should be synced.
     */
    boolean shouldReplicateConfigProperty(String prop);

    /**
     * Specifies how to replicate the given topic configuration property
     * that has a default value on the source cluster. Only invoked for properties
     * that {@link #shouldReplicateConfigProperty(String)} has returned
     * {@code true} for.
     *
     * @return {@code true} if the default value from the source topic should be synced
     * to the target topic, and {@code false} if the default value for the target topic
     * should be used instead
     */
    default boolean shouldReplicateSourceDefault(String prop) {
        return false;
    }

    default void close() {
        //nop
    }

    default void configure(Map<String, ?> props) {
        //nop
    }
}
