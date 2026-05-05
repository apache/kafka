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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

    private static final Logger log = LoggerFactory.getLogger(FailureInjectingTimeoutProcessorFactory.class);

    private int numTries = 0;

    private int failuresInjected = 0;

    @Override
    public KafkaAdminClient.TimeoutProcessor create(long now) {
        return new FailureInjectingTimeoutProcessor(now);
    }

    synchronized boolean shouldInjectFailure() {
        numTries++;
        if (numTries == 1) {
            failuresInjected++;
            return true;
        }
        return false;
    }

    public synchronized int failuresInjected() {
        return failuresInjected;
    }

    public final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
        public FailureInjectingTimeoutProcessor(long now) {
            super(now);
        }

        boolean callHasExpired(KafkaAdminClient.Call call) {
            if ((!call.isInternal()) && shouldInjectFailure()) {
                log.debug("Injecting timeout for {}.", call);
                return true;
            } else {
                boolean ret = super.callHasExpired(call);
                log.debug("callHasExpired({}) = {}", call, ret);
                return ret;
            }
        }
    }
}
