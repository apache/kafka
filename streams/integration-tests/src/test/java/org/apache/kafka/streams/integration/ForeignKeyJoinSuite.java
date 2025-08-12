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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.utils.BytesTest;
import org.apache.kafka.streams.kstream.internals.KTableKTableForeignKeyJoinScenarioTest;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySchemaTest;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ResponseJoinProcessorSupplierTest;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapperSerdeTest;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapperSerdeTest;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * This suite runs all the tests related to the KTable-KTable foreign key join feature.
 *
 * It can be used from an IDE to selectively just run these tests when developing code related to KTable-KTable
 * foreign key join.
 *
 * If desired, it can also be added to a Gradle build task, although this isn't strictly necessary, since all
 * these tests are already included in the `:streams:test` task.
 */
@Suite
@SelectClasses({
    BytesTest.class,
    KTableKTableForeignKeyInnerJoinMultiIntegrationTest.class,
    KTableKTableForeignKeyJoinIntegrationTest.class,
    KTableKTableForeignKeyJoinMaterializationIntegrationTest.class,
    KTableKTableForeignKeyJoinScenarioTest.class,
    CombinedKeySchemaTest.class,
    SubscriptionWrapperSerdeTest.class,
    SubscriptionResponseWrapperSerdeTest.class,
    ResponseJoinProcessorSupplierTest.class
})
public class ForeignKeyJoinSuite {
}
