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

package org.apache.kafka.coordinator.group;

import com.google.re2j.Pattern;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.PartitionRegistration;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupRegexManagerTest {

    private final MetadataImage metadataImage = mock(MetadataImage.class);
    private final MockCoordinatorTimer<Void, CoordinatorRecord> timer = new MockCoordinatorTimer<>(new MockTime());
    private final LogContext logContext = new LogContext();

    private static final TopicsImage TOPICS_IMAGE;
    private static final String TOPIC_NAME = "topic1";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    static {
        PartitionRegistration partitionRegistration = mock(PartitionRegistration.class);
        TOPICS_IMAGE = TopicsImage.EMPTY.including(new TopicImage(TOPIC_NAME, TOPIC_ID,
            Collections.singletonMap(0, partitionRegistration)));
    }

    @Test
    public void testValidPatternEvalRequested() {
        String groupId = "group1";
        String regex = "^t.*";
        GroupRegexManager regexManager = spy(createPatternManager(logContext, timer, metadataImage));
        when(metadataImage.topics()).thenReturn(TOPICS_IMAGE);
        Pattern pattern = assertDoesNotThrow(() -> regexManager.validateAndRequestEval(groupId, regex));
        verify(regexManager).maybeRequestEval(groupId, pattern);
    }

    @Test
    public void testNewMetadataImageDoesNotTriggerEval() {
        String groupId = "group1";
        String regex = "^t.*";
        GroupRegexManager regexManager = spy(createPatternManager(logContext, timer, metadataImage));
        when(metadataImage.topics()).thenReturn(TOPICS_IMAGE);
        Pattern pattern = assertDoesNotThrow(() -> regexManager.validateAndRequestEval(groupId, regex));
        verify(regexManager).maybeRequestEval(groupId, pattern);
        clearInvocations(regexManager);

        regexManager.onNewMetadataImage(metadataImage);
        verify(regexManager, never()).maybeRequestEval(any(), any());
    }

    public static GroupRegexManager createPatternManager(
        LogContext logContext,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        MetadataImage metadataImage
    ) {
        return new GroupRegexManager.Builder()
            .withLogContext(logContext)
            .withTimer(timer)
            .withMetadataImage(metadataImage)
            .build();
    }
}
