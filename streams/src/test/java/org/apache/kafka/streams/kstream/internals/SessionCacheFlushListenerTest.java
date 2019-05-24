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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

public class SessionCacheFlushListenerTest {
    @Test
    public void shouldForwardKeyNewValueOldValueAndTimestamp() {
        final InternalProcessorContext context = mock(InternalProcessorContext.class);
        expect(context.currentNode()).andReturn(null).anyTimes();
        context.setCurrentNode(null);
        context.setCurrentNode(null);
        context.forward(
            new Windowed<>("key", new SessionWindow(21L, 73L)),
            new Change<>("newValue", "oldValue"),
            To.all().withTimestamp(73L));
        expectLastCall();
        replay(context);

        new SessionCacheFlushListener<>(context).apply(
            new Windowed<>("key", new SessionWindow(21L, 73L)),
            "newValue",
            "oldValue",
            42L);

        verify(context);
    }
}
