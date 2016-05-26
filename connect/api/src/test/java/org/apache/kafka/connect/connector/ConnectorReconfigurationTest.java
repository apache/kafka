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

package org.apache.kafka.connect.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConnectorReconfigurationTest {

    @Test
    public void testDefaultReconfigure() throws Exception {
        TestConnector conn = new TestConnector(false);
        conn.reconfigure(Collections.<String, String>emptyMap());
        assertEquals(conn.stopOrder, 0);
        assertEquals(conn.configureOrder, 1);
    }

    @Test(expected = ConnectException.class)
    public void testReconfigureStopException() throws Exception {
        TestConnector conn = new TestConnector(true);
        conn.reconfigure(Collections.<String, String>emptyMap());
    }

    private static class TestConnector extends Connector {

        private boolean stopException;
        private int order = 0;
        public int stopOrder = -1;
        public int configureOrder = -1;

        public TestConnector(boolean stopException) {
            this.stopException = stopException;
        }

        @Override
        public String version() {
            return "1.0";
        }

        @Override
        public void start(Map<String, String> props) {
            configureOrder = order++;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return null;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int count) {
            return null;
        }

        @Override
        public void stop() {
            stopOrder = order++;
            if (stopException)
                throw new ConnectException("error");
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }
    }
}
