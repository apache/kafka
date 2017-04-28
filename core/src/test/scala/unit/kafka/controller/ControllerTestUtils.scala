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
