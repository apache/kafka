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
package org.apache.kafka.common.message;

import org.apache.kafka.message.MessageSpec;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test that {@link AbstractMessageCompatibilityTest#validateCompatibility(String, KafkaVersion, MessageSpec, KafkaVersion, MessageSpec)}
 * can detect incompatible changes.
 * This is basically a test for {@link AbstractMessageCompatibilityTest}.
 */
public class MetaCompatibilityTest extends AbstractMessageCompatibilityTest {

    @Test
    public void changedApiKeyIsInvalid() throws IOException {
        assertValidationFails("NewChangedApiKey.json",
                "SomeMessage: API key changed from Optional[15] in 1.0.0 to Optional[16] in 2.0.0 expected:<Optional[15]> but was:<Optional[16]>");
    }

    @Test
    public void changedLowerValidVersionsIsInvalid() throws IOException {
        assertValidationFails("NewChangedLowerValidVersions.json",
                "SomeMessage: Lowest supported version changed expected:<0> but was:<1>");
    }

    @Test
    public void fieldRemovedIsInvalid() throws IOException {
        assertValidationFails("NewRemovesField.json",
                "SomeMessage: Kafka version 2.0.0 removed fields [Bar] from API version 0");
    }

    @Test
    public void fieldsSwappedIsInvalid() throws IOException {
        assertValidationFails("NewChangesFieldOrder.json",
                "SomeMessage: Field 'Bar' was reordered in API version 0: it was after 'Foo' in 1.0.0 but it is before 'Foo' in 2.0.0");
    }

    @Test
    public void fieldDefaultChangedIsInvalid() throws IOException {
        assertValidationFails("NewChangedFieldDefault.json",
                "SomeMessage: Field 'Foo' in API version 0 has a different default in 1.0.0 than in 2.0.0 expected:<[]> but was:<[1]>");
    }

    @Test
    public void fieldDefaultAddedSameAsImplicitDefaultIsValid() throws IOException {
        assertValidationPasses("OldWithoutDefaults.json",
                "NewWithCompatibleDefaults.json");
    }

    @Test
    public void fieldTypeChangedIsInvalid() throws IOException {
        assertValidationFails("NewChangedFieldType.json",
                "SomeMessage: Field 'Foo' in API version 0 has a different type in 1.0.0 than in 2.0.0 expected:<[string]> but was:<[int32]>");
    }

    @Test
    public void fieldTypeNameChangedIsValid() throws IOException {
        assertValidationPasses("NewChangedFieldTypeName.json");
    }

    @Test
    public void fieldTypeNameAndStructureChangedIsInvalid() throws IOException {
        assertValidationFails("NewChangedFieldTypeNameAndStructure.json",
                "SomeMessage: Field 'Foo' in API version 0 has a different type in 1.0.0 than in 2.0.0 expected:<[string]> but was:<[int16]>");
    }

    @Test
    public void fieldTypeChangeFromArrayOfPrimitiveToArrayOfStructOfPrimitive() throws IOException {
        assertValidationPasses("OldWithArrayOfInt32.json",
                "NewWithArrayOfStructOfInt32.json");
    }

    @Test
    public void fieldNullabilityChangedIsInvalid() throws IOException {
        assertValidationFails("NewChangedFieldNullability.json",
                "SomeMessage: Field 'Foo' in API version 1 has a different nullability in 1.0.0 than in 2.0.0 expected:<[none]> but was:<[1]>");
    }

    @Test
    public void addTaggedFieldIsValid() throws IOException {
        assertValidationPasses("NewAddedTaggedField.json");
    }

    @Test
    public void addNonTaggedFieldIsInvalid() throws IOException {
        assertValidationFails("NewAddedNonTaggedField.json",
                "SomeMessage: Field 'Qux' added in API version 0/Kafka version 2.0.0 is not tagged and not ignorable");
    }

    @Test
    public void reusedTagIsInvalid() throws IOException {
        assertValidationFails("OldWithTag.json",
                "NewWithReusedTag.json",
                "SomeMessage: Field 'Qux' uses tag 1 but this was previously used for field 'Bar'");
    }
    // TODO added ignorable field is OK

    private void assertValidationFails(String newResource, String expectedMessage) throws IOException {
        assertValidationFails("OldMessage.json", newResource, expectedMessage);
    }

    KafkaVersion oldVersion = KafkaVersion.parse("1.0.0");
    KafkaVersion newVersion = KafkaVersion.parse("2.0.0");

    private void assertValidationFails(String oldResource, String newResource, String expectedMessage) throws IOException {
        MessageSpec oldSpec = loadMessageSpecResource(oldResource);
        MessageSpec newSpec = loadMessageSpecResource(newResource);
        try {
            validateCompatibility(oldSpec.name(), oldVersion, oldSpec, newVersion, newSpec);
            fail(newResource + " did not fail; was valid");
        } catch (AssertionError e) {
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    private void assertValidationPasses(String newResource) throws IOException {
        assertValidationPasses("OldMessage.json", newResource);
    }

    private void assertValidationPasses(String oldResource, String newResource) throws IOException {
        MessageSpec oldSpec = loadMessageSpecResource(oldResource);
        MessageSpec newSpec = loadMessageSpecResource(newResource);
        validateCompatibility(oldSpec.name(), oldVersion, oldSpec, newVersion, newSpec);
    }

    private MessageSpec loadMessageSpecResource(String newResource) throws IOException {
        MessageSpec newSpec;
        try (InputStream in = getClass().getResourceAsStream(newResource)) {
            newSpec = MAPPER.readValue(in, MessageSpec.class);
        }
        return newSpec;
    }
}
