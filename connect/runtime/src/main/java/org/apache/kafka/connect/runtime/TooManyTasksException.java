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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Thrown when a connector has generated too many task configs (i.e., more tasks than
 * the value for {@link ConnectorConfig#TASKS_MAX_CONFIG tasks.max} that it has
 * been configured with).
 */
public class TooManyTasksException extends ConnectException {

    public TooManyTasksException(String connName, int numTasks, int maxTasks) {
        super(String.format(
                "The connector %s has generated %d tasks, which is greater than %d, "
                        + "the maximum number of tasks it is configured to create. "
                        + "This behaviour should be considered a bug and is disallowed. "
                        + "If necessary, it can be permitted by reconfiguring the connector "
                        + "with '%s' set to false; however, this option will be removed in a "
                        + "future release of Kafka Connect.",
                connName,
                numTasks,
                maxTasks,
                ConnectorConfig.TASKS_MAX_ENFORCE_CONFIG
        ));
    }

}
