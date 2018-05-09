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
package kafka.controller

import org.easymock.{EasyMock, IAnswer}

object ControllerTestUtils {

  /** Since ControllerEvent is sealed, return a subclass of ControllerEvent created with EasyMock */
  def createMockControllerEvent(controllerState: ControllerState, process: () => Unit): ControllerEvent = {
    val mockEvent = EasyMock.createMock(classOf[ControllerEvent])
    EasyMock.expect(mockEvent.state).andReturn(controllerState)
    EasyMock.expect(mockEvent.process()).andAnswer(new IAnswer[Unit]() {
      def answer(): Unit = {
        process()
      }
    })
    EasyMock.replay(mockEvent)
    mockEvent
  }
}
