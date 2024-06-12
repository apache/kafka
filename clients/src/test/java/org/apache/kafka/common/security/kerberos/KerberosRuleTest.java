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
package org.apache.kafka.common.security.kerberos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class KerberosRuleTest {

    @Test
    public void testReplaceParameters() throws BadFormatString {
        // positive test cases
        assertEquals(KerberosRule.replaceParameters("", new String[0]), "");
        assertEquals(KerberosRule.replaceParameters("hello", new String[0]), "hello");
        assertEquals(KerberosRule.replaceParameters("", new String[]{"too", "many", "parameters", "are", "ok"}), "");
        assertEquals(KerberosRule.replaceParameters("hello", new String[]{"too", "many", "parameters", "are", "ok"}), "hello");
        assertEquals(KerberosRule.replaceParameters("hello $0", new String[]{"too", "many", "parameters", "are", "ok"}), "hello too");
        assertEquals(KerberosRule.replaceParameters("hello $0", new String[]{"no recursion $1"}), "hello no recursion $1");

        // negative test cases
        assertThrows(
            BadFormatString.class,
            () -> KerberosRule.replaceParameters("$0", new String[]{}),
            "An out-of-bounds parameter number should trigger an exception!");

        assertThrows(
            BadFormatString.class,
            () -> KerberosRule.replaceParameters("hello $a", new String[]{"does not matter"}),
            "A malformed parameter name should trigger an exception!");
    }

}
