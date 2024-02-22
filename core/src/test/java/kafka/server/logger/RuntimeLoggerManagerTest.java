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
package kafka.server.logger;

import kafka.utils.Log4jController;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RuntimeLoggerManagerTest {
    private final static Logger LOG = LoggerFactory.getLogger(RuntimeLoggerManagerTest.class);

    private final static RuntimeLoggerManager MANAGER = new RuntimeLoggerManager(5, LOG);

    @Test
    public void testValidateSetLogLevelConfig() {
        MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
            setName(LOG.getName()).
            setConfigOperation(OpType.SET.id()).
            setValue("TRACE")));
    }

    @Test
    public void testValidateDeleteLogLevelConfig() {
        MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
            setName(LOG.getName()).
            setConfigOperation(OpType.DELETE.id()).
            setValue("")));
    }

    @ParameterizedTest
    @ValueSource(bytes = {(byte) 2, (byte) 3})
    public void testOperationNotAllowed(byte id) {
        OpType opType = AlterConfigOp.OpType.forId(id);
        assertEquals(opType + " operation is not allowed for the BROKER_LOGGER resource",
            Assertions.assertThrows(InvalidRequestException.class,
                () -> MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
                    setName(LOG.getName()).
                    setConfigOperation(id).
                    setValue("TRACE")))).getMessage());
    }

    @Test
    public void testValidateBogusLogLevelNameNotAllowed() {
        assertEquals("Cannot set the log level of " + LOG.getName() + " to BOGUS as it is not " +
            "a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN",
            Assertions.assertThrows(InvalidConfigurationException.class,
                () -> MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
                    setName(LOG.getName()).
                    setConfigOperation(OpType.SET.id()).
                    setValue("BOGUS")))).getMessage());
    }

    @Test
    public void testValidateSetRootLogLevelConfig() {
        MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
                setName(Log4jController.ROOT_LOGGER()).
                setConfigOperation(OpType.SET.id()).
                setValue("TRACE")));
    }

    @Test
    public void testValidateRemoveRootLogLevelConfigNotAllowed() {
        assertEquals("Removing the log level of the " + Log4jController.ROOT_LOGGER() +
            " logger is not allowed",
            Assertions.assertThrows(InvalidRequestException.class,
                () -> MANAGER.validateLogLevelConfigs(Arrays.asList(new AlterableConfig().
                    setName(Log4jController.ROOT_LOGGER()).
                    setConfigOperation(OpType.DELETE.id()).
                    setValue("")))).getMessage());
    }
}
