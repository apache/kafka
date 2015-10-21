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

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time
 */
public interface Time {

    /**
     * The current time in milliseconds
     */
    long milliseconds();

    /**
     * The current time in nanoseconds
     */
    long nanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    void sleep(long ms);

}
