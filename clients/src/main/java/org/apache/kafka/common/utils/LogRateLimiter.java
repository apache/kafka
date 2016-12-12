/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.slf4j.Logger;

/**
 * Wrapper methods to log spammy messages only occasionally
 * Relies on the caller to provide basically everything and just does comparisons
 * to keep calling code a bit cleaner
 */
public class LogRateLimiter {

    /**
     * Log a warn level message with
     * org.slf4j.Logger#warn(java.lang.String, java.lang.Object, java.lang.Object)
     * @param logger logger to use
     * @param format see ref class
     * @param arg1 see ref class
     * @param arg2 see ref class
     * @param count number of times we've tried to log this message
     * @param max maximum number of times we try to log the message before suggesting a count reset
     * @return boolean indicating if the count should be reset or not
     */
    public static boolean warn(Logger logger, String format, Object arg1, Object arg2,
                            long count, long max){
        if (count == 0)
            logger.warn(format, arg1, arg2);
        if (count > max) {
            return true;
        }
        return false;
    }
}
