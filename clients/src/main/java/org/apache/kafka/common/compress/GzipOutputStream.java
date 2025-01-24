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
package org.apache.kafka.common.compress;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * An extension of {@link GZIPOutputStream}, with compression level functionality.
 */
public class GzipOutputStream extends GZIPOutputStream {
    /**
     * Creates a new {@link OutputStream} with the specified buffer size and compression level.
     *
     * @param out   the output stream
     * @param size  the output buffer size
     * @param level the compression level
     * @throws IOException If an I/O error has occurred.
     */
    public GzipOutputStream(OutputStream out, int size, int level) throws IOException {
        super(out, size);
        setLevel(level);
    }

    /**
     * Sets the compression level.
     *
     * @param level the compression level
     */
    private void setLevel(int level) {
        def.setLevel(level);
    }
}
