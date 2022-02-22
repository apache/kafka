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

    public static <T> T retry(Callable<T> callable, long maxRetries, long retryBackoffMs) throws Exception {
        Throwable lastError = null;
        int retries = 0;
        while (retries++ < maxRetries) {
            try {
                return callable.call();
            } catch (RetriableException | org.apache.kafka.connect.errors.RetriableException e) {
                log.warn("RetriableException caught, retrying automatically up to {} more times. " +
                        "Reason: {}", maxRetries - retries, e.getMessage());
                lastError = e;
            } catch (WakeupException e) {
                lastError = e;
            } catch (Exception e) {
                log.warn("Non-retriable exception caught. Re-throwing. Reason: {}, {}", e.getClass(), e.getMessage());
                throw e;
            }
            Utils.sleep(retryBackoffMs);
        }

        throw new ConnectException("Fail to retry the operation after " + maxRetries + " attempts.  Reason: " + lastError, lastError);
    }
}
