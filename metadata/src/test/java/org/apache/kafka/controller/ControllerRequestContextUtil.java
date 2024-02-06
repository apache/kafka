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

package org.apache.kafka.controller;

import java.util.OptionalLong;
import java.util.function.Consumer;

import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class ControllerRequestContextUtil {
    public static final ControllerRequestContext ANONYMOUS_CONTEXT =
        new ControllerRequestContext(
            new RequestHeaderData(),
            KafkaPrincipal.ANONYMOUS,
            OptionalLong.empty());
    public static final String QUOTA_EXCEEDED_IN_TEST_MSG = "Quota exceeded in test";

    public static ControllerRequestContext anonymousContextFor(ApiKeys apiKeys) {
        return anonymousContextFor(apiKeys, apiKeys.latestVersion(), __ -> { });
    }

    public static ControllerRequestContext anonymousContextWithMutationQuotaExceededFor(ApiKeys apiKeys) {
        return anonymousContextFor(apiKeys, apiKeys.latestVersion(), x -> {
            throw new ThrottlingQuotaExceededException(QUOTA_EXCEEDED_IN_TEST_MSG);
        });
    }

    public static ControllerRequestContext anonymousContextFor(
        ApiKeys apiKeys,
        short version
    ) {
        return anonymousContextFor(apiKeys, version, __ -> { });
    }

    public static ControllerRequestContext anonymousContextFor(
        ApiKeys apiKeys,
        short version,
        Consumer<Integer> partitionChangeQuotaApplier
    ) {
        return new ControllerRequestContext(
            new RequestHeaderData()
                .setRequestApiKey(apiKeys.id)
                .setRequestApiVersion(version),
            KafkaPrincipal.ANONYMOUS,
            OptionalLong.empty(),
            partitionChangeQuotaApplier
        );
    }
}
