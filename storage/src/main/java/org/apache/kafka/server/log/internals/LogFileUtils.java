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
package org.apache.kafka.server.log.internals;

public final class LogFileUtils {

    private LogFileUtils() {
    }

    /**
     * Returns the offset for the given file name. The file name is of the form: {number}.{suffix}. This method extracts
     * the number from the given file name.
     *
     * @param fileName name of the file
     * @return offset of the given file name
     */
    public static long offsetFromFileName(String fileName) {
        return Long.parseLong(fileName.substring(0, fileName.indexOf('.')));
    }

}
