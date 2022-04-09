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

/**
 * This class provides a way to instrument loggers with a common context which can be used to
 * automatically enrich log messages. For example, in the KafkaConsumer, it is often useful to know
 * the groupId of the consumer, so this can be added to a context object which can then be passed to
 * all of the dependent components in order to build new loggers. This removes the need to manually
 * add the groupId to each message.
 */
public class LogContext extends AbstractLogContext {

    private final String logPrefix;

    public LogContext(String logPrefix) {
        this.logPrefix = logPrefix == null ? "" : logPrefix;
    }

    public LogContext() {
        this("");
    }

    protected String addContext(final String message) {
        return logPrefix + message;
    }

    public String logPrefix() {
        return logPrefix;
    }

}
