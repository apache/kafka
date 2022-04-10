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

import java.util.function.Supplier;

/**
 * A log context whose prefix may vary across invocations, which can be used in cases where
 * contextual information changes over time within a single context. For example, in the
 * ConsumerCoordinator, it may be useful to know not only the client ID and group ID,
 * are static across the lifetime of the consumer, but also the current generation ID, which
 * changes over time.
 * */
public class DynamicLogContext extends AbstractLogContext {

    public DynamicLogContext(Supplier<String> logPrefix) {
        super(logPrefix == null ? null : message -> logPrefix.get() + message);
    }

}
