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
package org.apache.kafka.storage.internals.log;

/**
 * This class represents the verification state of a specific producer id.
 * It contains a verification guard object that is used to uniquely identify the transaction we want to verify.
 * After verifying, we retain this object until we append to the log. This prevents any race conditions where the transaction
 * may end via a control marker before we write to the log. This mechanism is used to prevent hanging transactions.
 * We remove the verification guard object whenever we write data to the transaction or write an end marker for the transaction.
 * Any lingering entries that are never verified are removed via the producer state entry cleanup mechanism.
 */
public class VerificationStateEntry {

    final private long timestamp;
    final private Object verificationGuard;

    public VerificationStateEntry(long timestamp) {
        this.timestamp = timestamp;
        this.verificationGuard = new Object();
    }

    public long timestamp() {
        return timestamp;
    }

    public Object verificationGuard() {
        return verificationGuard;
    }
}
