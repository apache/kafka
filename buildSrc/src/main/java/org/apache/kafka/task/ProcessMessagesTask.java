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

package org.apache.kafka.task;

import org.apache.kafka.message.MessageGenerator;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.Map;

/**
 * A gradle task which processes a directory full of JSON files into an output directory.
 */
public class ProcessMessagesTask extends DefaultTask {
    /**
     * The directory where we should read the input JSON from.
     */
    public File inputDirectory;

    /**
     * The directory that we should write output JSON to.
     */
    public File outputDirectory;

    @InputDirectory
    public File getInputDirectory() {
        return inputDirectory;
    }

    /**
     * Define the task outputs.
     *
     * Gradle consults this to see if the task is up-to-date.
     */
    @OutputFiles
    public Map<String, File> getOutputFiles() throws Exception {
        return MessageGenerator.getOutputFiles(
            outputDirectory.toString(), inputDirectory.toString());
    }

    @TaskAction
    public void run() {
        try {
            MessageGenerator.processDirectories(
                outputDirectory.toString(), inputDirectory.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
