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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

/**
 * CoordinatorShard is basically a replicated state machine managed by the
 * {@link CoordinatorRuntime}.
 */
public interface CoordinatorShard<U> extends CoordinatorPlayback<U> {

    /**
     * The coordinator has been loaded. This is used to apply any
     * post loading operations (e.g. registering timers).
     *
     * @param newImage  The metadata image.
     */
    default void onLoaded(MetadataImage newImage) {}

    /**
     * A new metadata image is available. This is only called after {@link CoordinatorShard#onLoaded(MetadataImage)}
     * is called to signal that the coordinator has been fully loaded.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    default void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {}

    /**
     * The coordinator has been unloaded. This is used to apply
     * any post unloading operations.
     */
    default void onUnloaded() {}
}
