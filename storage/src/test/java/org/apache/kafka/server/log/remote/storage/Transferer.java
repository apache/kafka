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
package org.apache.kafka.server.log.remote.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The implementation of the transfer of the data of the canonical segment and index files to
 * this storage. The only reason the "transferer" abstraction exists is to be able to simulate
 * file copy errors and exercise the associated failure modes.
 */
public interface Transferer {

    void transfer(File from, File to) throws IOException;

    void transfer(ByteBuffer from, File to) throws IOException;
}
