/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor;

import java.util.Map;

/**
 * A state store supplier which can create one or more {@link StateStore} instances.
 */
public interface StateStoreSupplier {

    /**
     * Return the name of this state store supplier.
     *
     * @return the name of this state store supplier
     */
    String name();

    /**
     * Return a new {@link StateStore} instance.
     *
     * @return  a new {@link StateStore} instance
     */
    StateStore get();

    /**
     * Returns a Map containing any log configs that will be used when creating the changelog for the {@link StateStore}
     *
     * Note: any unrecognized configs will be ignored by the Kafka brokers.
     * @return Map containing any log configs to be used when creating the changelog for the {@link StateStore}
     * If {@code loggingEnabled} returns false, this function will always return an empty map
     */
    Map<String, String> logConfig();

    /**
     * @return true if the {@link StateStore} should have logging enabled
     */
    boolean loggingEnabled();
}
