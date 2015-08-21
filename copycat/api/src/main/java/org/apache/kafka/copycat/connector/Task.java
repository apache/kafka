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
 **/

package org.apache.kafka.copycat.connector;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Properties;

/**
 * <p>
 * Tasks contain the code that actually copies data to/from another system. They receive
 * a configuration from their parent Connector, assigning them a fraction of a Copycat job's work.
 * The Copycat framework then pushes/pulls data from the Task. The Task must also be able to
 * respond to reconfiguration requests.
 * </p>
 * <p>
 * Task only contains the minimal shared functionality between
 * {@link org.apache.kafka.copycat.source.SourceTask} and
 * {@link org.apache.kafka.copycat.sink.SinkTask}.
 * </p>
 */
@InterfaceStability.Unstable
public interface Task {
    /**
     * Start the Task
     * @param props initial configuration
     */
    void start(Properties props);

    /**
     * Stop this task.
     */
    void stop();
}
