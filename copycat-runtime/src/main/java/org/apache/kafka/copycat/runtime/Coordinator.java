/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.copycat.util.Callback;

import java.util.Properties;

/**
 * The Coordinator interface works with coordinators on other workers to manage a set of Jobs.
 * Each job is a Connector instance with associated config and triggers tasks that are then run
 * in workers. The assignment and tracking of those tasks in workers is also managed by the
 * coordinator.
 */
public interface Coordinator {

    void start();

    void stop();

    /**
     * Submit a connector job to the cluster. This works from any node by forwarding the request to
     * the leader coordinator if necessary.
     *
     * @param connectorProps user-specified properties for this job
     * @param callback callback to invoke when the request completes
     */
    void addConnector(Properties connectorProps, Callback<String> callback);

    /**
     * Delete a connector job by name.
     *
     * @param name name of the connector job to shutdown and delete
     * @param callback callback to invoke when the request completes
     */
    void deleteConnector(String name, Callback<Void> callback);
}
