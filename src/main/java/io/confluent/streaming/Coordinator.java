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

package io.confluent.streaming;

/**
 * Coordinators are provided by the coordiantor methods of {@link KStreamContext} to allow
 * the user code to request actions from the framework.
 * <p>
 * This interface may evolve over time.
 * </p>
 */
public interface Coordinator {

    /**
     * Requests that the framework to flush all it's state as well as any buffered output and commit the offsets.
     *
     * <p>
     * Note that if you also have also configured your job to commit in regular intervals (using the
     * <code>task.commit.ms</code> property), those time-based commits are not affected by calling this method. Any
     * commits you request explicitly are in addition to timer-based commits. You can set <code>task.commit.ms=-1</code>
     * if you don't want commits to happen automatically.
     */
    void commit();

    /**
     * Requests that the framework to shut down.
     */
    void shutdown();

}
