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

package org.apache.kafka.copycat.runtime.standalone;

import java.util.Collection;
import java.util.Properties;

/**
 * Interface used by StandaloneController to store configuration data for jobs. To be fault
 * tolerant, all data required to resume jobs is stored here.
 */
public interface ConfigStorage {

    /**
     * Configure this storage engine.
     * @param props configuration properties
     */
    void configure(Properties props);

    /**
     * Close this storage engine.
     */
    void close();

    /**
     * Commit the new connector config.
     * @param connector
     * @param properties
     */
    void putConnectorConfig(String connector, Properties properties);

    /**
     * Read back the config for the given connector.
     * @param connector
     * @return
     */
    Properties getConnectorConfig(String connector);

    /**
     * Get a list of connector names that have associated state in the store.
     * @return
     */
    Collection<String> getConnectors();

}
