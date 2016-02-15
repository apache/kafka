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

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppInfoParser {
    private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);
    private static String version = "unknown";
    private static String commitId = "unknown";

    static {
        try (InputStream resourceStream = AppInfoParser.class.getResourceAsStream("/kafka/kafka-version.properties")) {
            Properties props = new Properties();
            props.load(resourceStream);
            version = props.getProperty("version", version).trim();
            commitId = props.getProperty("commitId", commitId).trim();
        } catch (Exception e) {
            log.warn("Error while loading kafka-version.properties :" + e.getMessage());
        }
    }

    public static String getVersion() {
        return version;
    }

    public static String getCommitId() {
        return commitId;
    }

    public static void registerAppInfo(String prefix, String id) {
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + id);
            AppInfo mBean = new AppInfo();
            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name);
        } catch (JMException e) {
            log.warn("Error registering AppInfo mbean", e);
        }
    }

    public static void unregisterAppInfo(String prefix, String id) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + id);
            if (server.isRegistered(name))
                server.unregisterMBean(name);
        } catch (JMException e) {
            log.warn("Error unregistering AppInfo mbean", e);
        }
    }

    public interface AppInfoMBean {
        public String getVersion();
        public String getCommitId();
    }

    public static class AppInfo implements AppInfoMBean {

        public AppInfo() {
            log.info("Kafka version : " + AppInfoParser.getVersion());
            log.info("Kafka commitId : " + AppInfoParser.getCommitId());
        }

        @Override
        public String getVersion() {
            return AppInfoParser.getVersion();
        }

        @Override
        public String getCommitId() {
            return AppInfoParser.getCommitId();
        }

    }
}
