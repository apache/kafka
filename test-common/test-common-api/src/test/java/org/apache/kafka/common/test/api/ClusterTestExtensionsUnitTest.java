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

package org.apache.kafka.common.test.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterTestExtensionsUnitTest {

    static List<ClusterConfig> cfgEmpty() {
        return Collections.emptyList();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ExtensionContext buildExtensionContext(String methodName) throws Exception {
        ExtensionContext extensionContext = mock(ExtensionContext.class);
        Class clazz = ClusterTestExtensionsUnitTest.class;
        Method method = clazz.getDeclaredMethod(methodName);
        when(extensionContext.getRequiredTestClass()).thenReturn(clazz);
        when(extensionContext.getRequiredTestMethod()).thenReturn(method);
        return extensionContext;
    }

    @Test
    void testProcessClusterTemplate() throws Exception {
        ClusterTestExtensions ext = new ClusterTestExtensions();
        ExtensionContext context = buildExtensionContext("cfgEmpty");

        ClusterTemplate annot = mock(ClusterTemplate.class);
        when(annot.value()).thenReturn("").thenReturn(" ").thenReturn("cfgEmpty");

        Assertions.assertEquals(
                "ClusterTemplate value can't be empty string.",
                Assertions.assertThrows(IllegalStateException.class, () ->
                        ext.processClusterTemplate(context, annot)
                ).getMessage()
        );


        Assertions.assertEquals(
                "ClusterTemplate value can't be empty string.",
                Assertions.assertThrows(IllegalStateException.class, () ->
                        ext.processClusterTemplate(context, annot)
                ).getMessage()
        );

        Assertions.assertEquals(
                "ClusterConfig generator method should provide at least one config",
                Assertions.assertThrows(IllegalStateException.class, () ->
                        ext.processClusterTemplate(context, annot)
                ).getMessage()
        );
    }
}
