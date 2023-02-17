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
package org.apache.kafka.storage.internals.log;

import java.io.File;
import java.text.NumberFormat;

public final class LogFileUtils {

    /**
     * Suffix of a producer snapshot file
     */
    public static final String PRODUCER_SNAPSHOT_FILE_SUFFIX = ".snapshot";

    /**
     * Suffix for a file that is scheduled to be deleted
     */
    public static final String DELETED_FILE_SUFFIX = ".deleted";

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

    /**
     * Returns a File instance with parent directory as logDir and the file name as producer snapshot file for the
     * given offset.
     *
     * @param logDir The directory in which the log will reside
     * @param offset The last offset (exclusive) included in the snapshot
     * @return a File instance for producer snapshot.
     */
    public static File producerSnapshotFile(File logDir, long offset) {
        return new File(logDir, filenamePrefixFromOffset(offset) + PRODUCER_SNAPSHOT_FILE_SUFFIX);
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    private static String filenamePrefixFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

}
