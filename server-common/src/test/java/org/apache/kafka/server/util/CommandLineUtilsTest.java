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
package org.apache.kafka.server.util;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CommandLineUtilsTest {
    @Test
    public void testParseEmptyArg() {
        List<String> argArray = Arrays.asList("my.empty.property=");

        assertThrows(IllegalArgumentException.class, () -> CommandLineUtils.parseKeyValueArgs(argArray, false));
    }

    @Test
    public void testParseEmptyArgWithNoDelimiter() {
        List<String> argArray = Arrays.asList("my.empty.property");

        assertThrows(IllegalArgumentException.class, () -> CommandLineUtils.parseKeyValueArgs(argArray, false));
    }

    @Test
    public void testParseEmptyArgAsValid() {
        List<String> argArray = Arrays.asList("my.empty.property=", "my.empty.property1");
        Properties props = CommandLineUtils.parseKeyValueArgs(argArray);

        assertEquals(props.getProperty("my.empty.property"), "", "Value of a key with missing value should be an empty string");
        assertEquals(props.getProperty("my.empty.property1"), "", "Value of a key with missing value with no delimiter should be an empty string");
    }

    @Test
    public void testParseSingleArg() {
        List<String> argArray = Arrays.asList("my.property=value");
        Properties props = CommandLineUtils.parseKeyValueArgs(argArray);

        assertEquals(props.getProperty("my.property"), "value", "Value of a single property should be 'value'");
    }

    @Test
    public void testParseArgs() {
        List<String> argArray = Arrays.asList("first.property=first", "second.property=second");
        Properties props = CommandLineUtils.parseKeyValueArgs(argArray);

        assertEquals(props.getProperty("first.property"), "first", "Value of first property should be 'first'");
        assertEquals(props.getProperty("second.property"), "second", "Value of second property should be 'second'");
    }

    @Test
    public void testParseArgsWithMultipleDelimiters() {
        List<String> argArray = Arrays.asList("first.property==first", "second.property=second=", "third.property=thi=rd");
        Properties props = CommandLineUtils.parseKeyValueArgs(argArray);

        assertEquals(props.getProperty("first.property"), "=first", "Value of first property should be '=first'");
        assertEquals(props.getProperty("second.property"), "second=", "Value of second property should be 'second='");
        assertEquals(props.getProperty("third.property"), "thi=rd", "Value of second property should be 'thi=rd'");
    }

    Properties props = new Properties();
    OptionParser parser = new OptionParser(false);
    OptionSpec<String> stringOpt;
    OptionSpec<Integer> intOpt;
    OptionSpec<String> stringOptOptionalArg;
    OptionSpec<Integer> intOptOptionalArg;
    OptionSpec<String> stringOptOptionalArgNoDefault;
    OptionSpec<Integer> intOptOptionalArgNoDefault;

    private void setUpOptions() {
        stringOpt = parser.accepts("str")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("default-string");
        intOpt = parser.accepts("int")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(100);
        stringOptOptionalArg = parser.accepts("str-opt")
                .withOptionalArg()
                .ofType(String.class)
                .defaultsTo("default-string-2");
        intOptOptionalArg = parser.accepts("int-opt")
                .withOptionalArg()
                .ofType(Integer.class)
                .defaultsTo(200);
        stringOptOptionalArgNoDefault = parser.accepts("str-opt-nodef")
                .withOptionalArg()
                .ofType(String.class);
        intOptOptionalArgNoDefault = parser.accepts("int-opt-nodef")
                .withOptionalArg()
                .ofType(Integer.class);
    }

    @Test
    public void testMaybeMergeOptionsOverwriteExisting() {
        setUpOptions();

        props.put("skey", "existing-string");
        props.put("ikey", "300");
        props.put("sokey", "existing-string-2");
        props.put("iokey", "400");
        props.put("sondkey", "existing-string-3");
        props.put("iondkey", "500");

        OptionSet options = parser.parse(
                "--str", "some-string",
                "--int", "600",
                "--str-opt", "some-string-2",
                "--int-opt", "700",
                "--str-opt-nodef", "some-string-3",
                "--int-opt-nodef", "800"
        );

        CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt);
        CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt);
        CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault);
        CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault);

        assertEquals("some-string", props.get("skey"));
        assertEquals("600", props.get("ikey"));
        assertEquals("some-string-2", props.get("sokey"));
        assertEquals("700", props.get("iokey"));
        assertEquals("some-string-3", props.get("sondkey"));
        assertEquals("800", props.get("iondkey"));
    }

    @Test
    public void testMaybeMergeOptionsDefaultOverwriteExisting() {
        setUpOptions();

        props.put("sokey", "existing-string");
        props.put("iokey", "300");
        props.put("sondkey", "existing-string-2");
        props.put("iondkey", "400");

        OptionSet options = parser.parse(
                "--str-opt",
                "--int-opt",
                "--str-opt-nodef",
                "--int-opt-nodef"
        );

        CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault);
        CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault);

        assertEquals("default-string-2", props.get("sokey"));
        assertEquals("200", props.get("iokey"));
        assertNull(props.get("sondkey"));
        assertNull(props.get("iondkey"));
    }

    @Test
    public void testMaybeMergeOptionsDefaultValueIfNotExist() {
        setUpOptions();

        OptionSet options = parser.parse();

        CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt);
        CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt);
        CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault);
        CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault);

        assertEquals("default-string", props.get("skey"));
        assertEquals("100", props.get("ikey"));
        assertEquals("default-string-2", props.get("sokey"));
        assertEquals("200", props.get("iokey"));
        assertNull(props.get("sondkey"));
        assertNull(props.get("iondkey"));
    }

    @Test
    public void testMaybeMergeOptionsNotOverwriteExisting() {
        setUpOptions();

        props.put("skey", "existing-string");
        props.put("ikey", "300");
        props.put("sokey", "existing-string-2");
        props.put("iokey", "400");
        props.put("sondkey", "existing-string-3");
        props.put("iondkey", "500");

        OptionSet options = parser.parse();

        CommandLineUtils.maybeMergeOptions(props, "skey", options, stringOpt);
        CommandLineUtils.maybeMergeOptions(props, "ikey", options, intOpt);
        CommandLineUtils.maybeMergeOptions(props, "sokey", options, stringOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "iokey", options, intOptOptionalArg);
        CommandLineUtils.maybeMergeOptions(props, "sondkey", options, stringOptOptionalArgNoDefault);
        CommandLineUtils.maybeMergeOptions(props, "iondkey", options, intOptOptionalArgNoDefault);

        assertEquals("existing-string", props.get("skey"));
        assertEquals("300", props.get("ikey"));
        assertEquals("existing-string-2", props.get("sokey"));
        assertEquals("400", props.get("iokey"));
        assertEquals("existing-string-3", props.get("sondkey"));
        assertEquals("500", props.get("iondkey"));
    }

    static private Properties createTestProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "this");
        props.setProperty("bootstrap.controllers", "that");
        return props;
    }

    @Test
    public void testInitializeBootstrapPropertiesWithNoBootstraps() {
        assertEquals("You must specify either --bootstrap-controller or --bootstrap-server.",
            assertThrows(CommandLineUtils.InitializeBootstrapException.class,
                () -> CommandLineUtils.initializeBootstrapProperties(createTestProps(),
                    Optional.empty(), Optional.empty())).getMessage());
    }

    @Test
    public void testInitializeBootstrapPropertiesWithBrokerBootstrap() {
        Properties props = createTestProps();
        CommandLineUtils.initializeBootstrapProperties(props,
            Optional.of("127.0.0.2:9094"), Optional.empty());
        assertEquals("127.0.0.2:9094", props.getProperty("bootstrap.servers"));
        assertNull(props.getProperty("bootstrap.controllers"));
    }

    @Test
    public void testInitializeBootstrapPropertiesWithControllerBootstrap() {
        Properties props = createTestProps();
        CommandLineUtils.initializeBootstrapProperties(props,
            Optional.empty(), Optional.of("127.0.0.2:9094"));
        assertNull(props.getProperty("bootstrap.servers"));
        assertEquals("127.0.0.2:9094", props.getProperty("bootstrap.controllers"));
    }

    @Test
    public void testInitializeBootstrapPropertiesWithBothBootstraps() {
        assertEquals("You cannot specify both --bootstrap-controller and --bootstrap-server.",
            assertThrows(CommandLineUtils.InitializeBootstrapException.class,
                () -> CommandLineUtils.initializeBootstrapProperties(createTestProps(),
                    Optional.of("127.0.0.2:9094"), Optional.of("127.0.0.3:9095"))).getMessage());
    }
}
