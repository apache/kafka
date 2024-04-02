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

package org.apache.kafka.server.group.share;

/**
 * Marker interface for result classes related to the
 * persister result classes. In this impl (KIP-932), the persister will invoke
 * the share coordinator to store state information. However, this could change
 * in the future where some other strategy could be used to persist state. Hence,
 * we do not want to expose the persister RPCs to the external callers.
 * To get around this we introduce a few DTO classes to serve as the arguments
 * and return types of the DefaultStatePersister class.
 */
public interface PersisterResult {
}
