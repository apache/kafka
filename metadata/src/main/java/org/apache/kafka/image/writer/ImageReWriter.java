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

package org.apache.kafka.image.writer;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;


/**
 * ImageReWriter writes a metadata image out to another metadata image.
 *
 * There are a few reasons why you might want to do this. One is to obtain a MetadataDelta
 * object which contains everything in the image. Another is to translate an image from
 * one metadata version to another.
 */
public class ImageReWriter implements ImageWriter {
    private final MetadataDelta delta;
    private boolean closed = false;
    private MetadataImage image = null;

    public ImageReWriter(MetadataDelta delta) {
        this.delta = delta;
    }

    @Override
    public void write(ApiMessageAndVersion record) {
        if (closed) throw new ImageWriterClosedException();
        delta.replay(record.message());
    }

    @Override
    public void close(boolean complete) {
        if (closed) return;
        closed = true;
        if (complete) {
            image = delta.apply(delta.image().provenance());
        }
    }

    public MetadataImage image() {
        return image;
    }
}
