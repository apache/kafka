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
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.FetchRequestUtils;

public enum FetchIsolation {
    LOG_END,
    HIGH_WATERMARK,
    TXN_COMMITTED;

    public static FetchIsolation apply(FetchRequest request) {
        return apply(request.replicaId(), request.isolationLevel());
    }

    public static FetchIsolation apply(int replicaId, IsolationLevel isolationLevel) {
        if (!FetchRequestUtils.isConsumer(replicaId)) {
            return LOG_END;
        } else if (isolationLevel == IsolationLevel.READ_COMMITTED) {
            return TXN_COMMITTED;
        } else {
            return HIGH_WATERMARK;
        }
    }
}
