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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.Uuid;

/**
 * Provide the default directory for new partitions in a given broker.
 * For brokers that are registered with multiple directories, the return value
 * should always be {@link org.apache.kafka.common.DirectoryId#UNASSIGNED}.
 * For brokers that are registered with a single log directory, then the return
 * value should be the ID for that directory.
 */
@FunctionalInterface
public interface DefaultDirProvider {
    Uuid defaultDir(int brokerId);
}
