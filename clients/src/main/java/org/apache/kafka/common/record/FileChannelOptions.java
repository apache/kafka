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
package org.apache.kafka.common.record;

class FileChannelOptions {
    private final boolean mutable;
    private final boolean fileAlreadyExists;
    private final int initFileSize;
    private final boolean preallocate;

    FileChannelOptions(boolean mutable,
                       boolean fileAlreadyExists,
                       int initFileSize,
                       boolean preallocate) {
        this.mutable = mutable;
        this.fileAlreadyExists = fileAlreadyExists;
        this.initFileSize = initFileSize;
        this.preallocate = preallocate;
    }

    boolean isMutable() {
        return mutable;
    }

    boolean isFileAlreadyExists() {
        return fileAlreadyExists;
    }

    int getInitFileSize() {
        return initFileSize;
    }

    boolean isPreallocate() {
        return preallocate;
    }

    FileChannelOptions withFileAlreadyExists(boolean fileAlreadyExists) {
        return new FileChannelOptions(mutable,
            fileAlreadyExists,
            initFileSize,
            preallocate
        );
    }
}
