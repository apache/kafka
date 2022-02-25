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

import java.util.concurrent.Callable;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * The method executes the callable at least once, optionally retrying the callable if
     * {@link org.apache.kafka.connect.errors.RetriableException} is being thrown.  If all retries are exhausted,
     * then the last exception is wrapped with a {@link org.apache.kafka.connect.errors.ConnectException} and thrown.
     *
     * <p>If {@code maxRetries} is set to 0, the task will be
     * executed exactly once.  If {@code maxRetries} is set to ,{@code n} the callable will be executed at
     * most {@code n + 1} times.
     *
     * <p>If {@code retryBackoffMs} is set to 0, no wait will happen in between the retries.
     *
     * @param callable the function to execute.
     * @param maxRetries maximum number of retries; must be 0 or more
     * @param retryBackoffMs the number of milliseconds to delay upon receiving a
     * {@link org.apache.kafka.connect.errors.RetriableException} before retrying again; must be 0 or more
     *
     * @throws ConnectException If the task exhausted all the retries.
     */
    public static <T> T retry(Callable<T> callable, long maxRetries, long retryBackoffMs) throws Exception {
        if (maxRetries <= 0) {
            // no retry
            return callable.call();
        }

        Throwable lastError = null;
        int attempt = 0;
        final long maxAttempts = maxRetries + 1;
        while (++attempt <= maxAttempts) {
            try {
                return callable.call();
            } catch (RetriableException | org.apache.kafka.connect.errors.RetriableException e) {
                log.warn("RetriableException caught on attempt {}, retrying automatically up to {} more times. " +
                        "Reason: {}", attempt, maxRetries - attempt, e.getMessage(), e);
                lastError = e;
            } catch (WakeupException e) {
                lastError = e;
            }

            if (attempt < maxAttempts) {
                Utils.sleep(retryBackoffMs);
            }
        }

        throw new ConnectException("Fail to retry the task after " + maxAttempts + " attempts.  Reason: " + lastError.getMessage(), lastError);
    }
}
