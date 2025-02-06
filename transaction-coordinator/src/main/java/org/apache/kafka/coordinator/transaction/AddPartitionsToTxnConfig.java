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

package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public final class AddPartitionsToTxnConfig {
    // The default config values for the server-side add partition to transaction operations.
    public static final String ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_CONFIG = "add.partitions.to.txn.retry.backoff.ms";
    public static final int ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_DEFAULT = 20;
    public static final String ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_DOC = "The retry backoff when the server attempts" +
        "to add the partition to the transaction";

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
        .define(ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_CONFIG, INT, ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_DEFAULT, atLeast(1), HIGH, ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_DOC);

    private final int addPartitionsToTxnRetryBackoffMs;

    public AddPartitionsToTxnConfig(AbstractConfig config) {
        addPartitionsToTxnRetryBackoffMs = config.getInt(AddPartitionsToTxnConfig.ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_CONFIG);
    }
    public int addPartitionsToTxnRetryBackoffMs() {
        return addPartitionsToTxnRetryBackoffMs;
    }
}
