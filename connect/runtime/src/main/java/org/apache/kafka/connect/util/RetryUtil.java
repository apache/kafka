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
package org.apache.kafka.connect.util;

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * The method executes the callable at least once, optionally retrying the callable if
     * {@link org.apache.kafka.connect.errors.RetriableException} is being thrown.  If timeout is exhausted,
     * then the last exception is wrapped with a {@link org.apache.kafka.connect.errors.ConnectException} and thrown.
     *
     * <p>{@code description} supplies the message that indicates the purpose of the callable since the message will
     * be used for logging.  For example, "list offsets". If the supplier is null or the supplied string is
     * null, {@call callable} will be used as the default string.
     *
     * <p>If {@code timeoutDuration} is set to 0, the task will be
     * executed exactly once.  If {@code timeoutDuration} is less than {@code retryBackoffMs}, the callable will be
     * executed only once.
     *
     * <p>If {@code retryBackoffMs} is set to 0, no wait will happen in between the retries.
     *
     * @param callable          the function to execute.
     * @param description       supplier that provides custom message for logging purpose
     * @param timeoutDuration   timeout duration; may not be null
     * @param retryBackoffMs    the number of milliseconds to delay upon receiving a
     *                          {@link org.apache.kafka.connect.errors.RetriableException} before retrying again;
     *                          must be 0 or more
     * @throws ConnectException If the task exhausted all the retries.
     */
    public static <T> T retryUntilTimeout(Callable<T> callable, Supplier<String> description, Duration timeoutDuration, long retryBackoffMs) throws Exception {
        if (timeoutDuration == null) {
            throw new IllegalArgumentException("timeoutDuration cannot be null");
        }

        // if null supplier or string is provided, the message will be default to "callabe"
        String descriptionStr = Optional.ofNullable(description)
                .map(Supplier::get)
                .orElse("callable");

        long timeoutMs = timeoutDuration.toMillis();

        if (retryBackoffMs >= timeoutMs) {
            log.warn("Call to {} will only execute once, since retryBackoffMs={} is larger than total timeoutMs={}",
                    descriptionStr, retryBackoffMs, timeoutMs);
        }

        if (retryBackoffMs >= timeoutMs ||
                timeoutMs <= 0) {
            // no retry
            return callable.call();
        }

        long end = System.currentTimeMillis() + timeoutMs;
        int attempt = 0;
        Throwable lastError = null;
        while (System.currentTimeMillis() <= end) {
            attempt++;
            try {
                return callable.call();
            } catch (RetriableException | org.apache.kafka.connect.errors.RetriableException e) {
                log.warn("Attempt {} to execute {} resulted in RetriableException; retrying automatically. " +
                        "Reason: {}", attempt, descriptionStr, e.getMessage(), e);
                lastError = e;
            } catch (WakeupException e) {
                lastError = e;
            }

            // if current time is less than the ending time, no more retry is necessary
            // won't sleep if retryBackoffMs equals to 0
            long millisRemaining = Math.max(0, end - System.currentTimeMillis());
            if (millisRemaining > 0) {
                Utils.sleep(retryBackoffMs);
            }
        }

        throw new ConnectException("Fail to execute " + descriptionStr + " after " + attempt + " attempts.  Reason: " + lastError.getMessage(), lastError);
    }
}
