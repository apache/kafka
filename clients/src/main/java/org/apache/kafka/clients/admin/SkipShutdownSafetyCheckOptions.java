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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link Admin#skipShutdownSafetyCheck(SkipShutdownSafetyCheckOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public final class SkipShutdownSafetyCheckOptions extends AbstractOptions<SkipShutdownSafetyCheckOptions> {
    private int brokerId;
    private long brokerEpoch;

    public int brokerId() {
        return brokerId;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public SkipShutdownSafetyCheckOptions brokerId(int brokerId) {
        this.brokerId = brokerId;
        return this;
    }

    public SkipShutdownSafetyCheckOptions brokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
        return this;
    }
}
