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
package org.apache.kafka.common.utils;

public class FetchRequestUtils {
    public static final int ORDINARY_CONSUMER_ID = -1;
    public static final int DEBUGGING_CONSUMER_ID = -2;
    public static final int FUTURE_LOCAL_REPLICA_ID = -3;

    // Broker ids are non-negative int.
    public static boolean isValidBrokerId(int brokerId) {
        return brokerId >= 0;
    }

    public static boolean isConsumer(int replicaId) {
        return replicaId < 0 && replicaId != FUTURE_LOCAL_REPLICA_ID;
    }

    public static String describeReplicaId(int replicaId) {
        switch (replicaId) {
            case ORDINARY_CONSUMER_ID: return "consumer";
            case DEBUGGING_CONSUMER_ID: return "debug consumer";
            case FUTURE_LOCAL_REPLICA_ID: return "future local replica";
            default: {
                if (isValidBrokerId(replicaId)) {
                    return String.format("replica [%s]", replicaId);
                } else {
                    return String.format("invalid replica [%s]", replicaId);
                }
            }
        }
    }
}
