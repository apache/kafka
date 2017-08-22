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
package org.apache.kafka.common.utils;

import java.util.StringTokenizer;

public final class Java {

    private Java() {
    }

    public static final String JVM_SPEC_VERSION = System.getProperty("java.specification.version");

    private static final int JVM_MAJOR_VERSION;
    private static final int JVM_MINOR_VERSION;

    static {
        final StringTokenizer st = new StringTokenizer(JVM_SPEC_VERSION, ".");
        JVM_MAJOR_VERSION = Integer.parseInt(st.nextToken());
        if (st.hasMoreTokens()) {
            JVM_MINOR_VERSION = Integer.parseInt(st.nextToken());
        } else {
            JVM_MINOR_VERSION = 0;
        }
    }

    public static final boolean IS_JAVA9_COMPATIBLE = JVM_MAJOR_VERSION > 1 ||
            (JVM_MAJOR_VERSION == 1 && JVM_MINOR_VERSION >= 9);

    public static final boolean IS_JAVA8_COMPATIBLE = JVM_MAJOR_VERSION > 1 ||
            (JVM_MAJOR_VERSION == 1 && JVM_MINOR_VERSION >= 8);

    public static boolean isIBMJdk() {
        return System.getProperty("java.vendor").contains("IBM");
    }

}
