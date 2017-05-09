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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class Connectors extends AbstractModuleFactory<Connector> {
    private static final Logger log = LoggerFactory.getLogger(Connectors.class);

    public Connectors(DelegatingClassLoader loader) {
        super(loader);
    }

    public Connector newConnector(String connectorClassOrAlias) {
        return newModule(connectorClassOrAlias, Connector.class);
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return instantiate(taskClass);
    }

    private static String connectorNames(Collection<Class<? extends Connector>> connectors) {
        StringBuilder names = new StringBuilder();
        for (Class<?> c : connectors)
            names.append(c.getName()).append(", ");
        return names.substring(0, names.toString().length() - 2);
    }

}
