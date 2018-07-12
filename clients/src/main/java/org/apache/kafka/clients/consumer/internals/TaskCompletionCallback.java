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
package org.apache.kafka.clients.consumer.internals;

/**
 * A class which is used in RebalanceKafkaConsumer for method callbacks once
 * a particular class specified by the parent consumer is complete. We could
 * redefine the method to suit our own purposes.
 */
public abstract class TaskCompletionCallback {
    /**
     * A method which is used by KafkaConsumer for specific tasks.
     *
     * @param result the value returned by the method upon completion of the task.
     *               If it is null, it means there is no return value.
     */
    public abstract void onTaskComplete(RebalanceKafkaConsumer.RequestResult result);
}
