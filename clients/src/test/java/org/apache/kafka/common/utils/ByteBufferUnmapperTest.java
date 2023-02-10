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

import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ByteBufferUnmapperTest {

    /**
     * Checks that unmap doesn't throw exceptions.
     */
    @Test
    public void testUnmap() throws Exception {
        File file = TestUtils.tempFile();
        try (FileChannel channel = FileChannel.open(file.toPath())) {
            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, 0);
            ByteBufferUnmapper.unmap(file.getAbsolutePath(), map);
        }
    }

}
