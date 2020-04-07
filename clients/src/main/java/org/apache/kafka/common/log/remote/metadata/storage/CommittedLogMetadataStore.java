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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class CommittedLogMetadataStore {
    private final File metadataStoreFile;

    CommittedLogMetadataStore(File metadataStoreFile) {
        this.metadataStoreFile = metadataStoreFile;

        if (!metadataStoreFile.exists()) {
            try {
                metadataStoreFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void write(Collection<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas)
            throws IOException {
        File newMetadataStoreFile = new File(metadataStoreFile.getAbsolutePath() + ".new");
        try (FileOutputStream fileOutputStream = new FileOutputStream(newMetadataStoreFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
             RLSMSerDe.RLSMSerializer serializer = new RLSMSerDe.RLSMSerializer()) {

            //write version
            ByteBuffer versionBuffer = ByteBuffer.allocate(2);
            versionBuffer.putShort((short) 0);
            bufferedOutputStream.write(versionBuffer.array());

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);

            // write each entry
            for (RemoteLogSegmentMetadata remoteLogSegmentMetadata : remoteLogSegmentMetadatas) {
                final byte[] serializedBytes = serializer.serialize(null, remoteLogSegmentMetadata, false);
                // write length
                lenBuffer.putInt(serializedBytes.length);
                bufferedOutputStream.write(lenBuffer.array());
                lenBuffer.flip();

                //write data
                bufferedOutputStream.write(serializedBytes);
            }

            fileOutputStream.getFD().sync();
        }

        Utils.atomicMoveWithFallback(newMetadataStoreFile.toPath(), metadataStoreFile.toPath());
    }

    @SuppressWarnings("unchecked")
    public synchronized Collection<RemoteLogSegmentMetadata> read() throws IOException {

        // checking for empty files.
        if (metadataStoreFile.length() == 0) {
            return Collections.emptyList();
        }

        try (FileInputStream fis = new FileInputStream(metadataStoreFile);
             RLSMSerDe.RLSMDeserializer deserializer = new RLSMSerDe.RLSMDeserializer()) {

            List<RemoteLogSegmentMetadata> result = new ArrayList<>();

            // read version
            ByteBuffer versionBytes = ByteBuffer.allocate(2);
            fis.read(versionBytes.array());
            short version = versionBytes.getShort();

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);

            //read the length of each entry
            while (fis.read(lenBuffer.array()) != -1) {
                final int len = lenBuffer.getInt();
                lenBuffer.flip();

                //read the entry
                byte[] data = new byte[len];
                final int read = fis.read(data);
                if (read != len) {
                    throw new IOException("Invalid amount of data read, file may have been corrupted.");
                }

                final RemoteLogSegmentMetadata rlsm = deserializer.deserialize(null, version,
                        ByteBuffer.wrap(data));
                result.add(rlsm);
            }

            return result;
        }
    }

}
