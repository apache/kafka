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
package org.apache.kafka.common.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Provides an interface to setup MBean if log4j is available in the classpath.
 */
public class LogLevelManager {

    private static final Logger log = LoggerFactory.getLogger(LogLevelManager.class);

    public static void registerLog4jController(String applicationName) {
        try {
            Class.forName("org.apache.log4j.Logger");
        } catch (ClassNotFoundException ce) {
            log.warn("Log4j was not found in classpath. Not registering Log4jController MBean.");
            return;
        }

        try {
            Object mbean = new Log4jController();
            String name = String.format("%s:type=Log4jController", applicationName);
            ObjectName objName = new ObjectName(name);
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, objName);
            log.info("Registered {} MBean", name);
        } catch (Exception e) {
            log.error("Could not register mbean for application" + applicationName, e);
        }
    }
}
