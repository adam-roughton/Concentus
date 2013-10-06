package com.adamroughton.concentus.crowdhammer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.curator.framework.listen.Listenable;

public interface TestTask extends Runnable {

	enum State {
		CREATED,
		SETTING_UP,
		RUNNING_TEST,
		TEARING_DOWN,
		STOPPED
	}
	
	String getTestName();
	
	void stop();
	
	State getState();
	
	void waitForState(State state) throws InterruptedException;
	
	void waitForState(State state, long timeout, TimeUnit unit) throws InterruptedException, 
		TimeoutException;
	
	Listenable<TestTaskListener> getListenable();
	
	public static final class TestRunInfo {
		public final String testName;
		public final String deploymentName; 
		public final int clientCount;
		
		public TestRunInfo(String testName, String deploymentName, int clientCount) {
			this.testName = testName;
			this.deploymentName = deploymentName;
			this.clientCount = clientCount;
		}
		
		public TestRunInfo(String testName, String deploymentName) {
			this(testName, deploymentName, 0);
		}
		
		@Override
		public String toString() {
			return testName + "." + deploymentName + (clientCount == 0? "" : "." + clientCount);
		}
		
		public TestRunInfo withClientCount(int clientCount) {
			return new TestRunInfo(testName, deploymentName, clientCount);
		}
		
		public TestRunInfo withNoClients() {
			return new TestRunInfo(testName, deploymentName);
		}
	}
	
	public static class TestTaskEvent {
		
		public enum EventType {
			STATE_CHANGE,
			PROGRESS_MESSAGE
		}
		
		private final EventType _eventType;
		private final TestRunInfo _currentRun;
		private final State _state;
		private final String _message;
		private final Exception _exception;
		
		public TestTaskEvent(TestRunInfo currentRun, State state, Exception exception) {
			this(EventType.STATE_CHANGE, currentRun, Objects.requireNonNull(state), null, exception);
		}
		
		public TestTaskEvent(TestRunInfo currentRun, String message) {
			this(EventType.PROGRESS_MESSAGE, currentRun, null, message, null);
		}
		
		public TestTaskEvent(EventType eventType, TestRunInfo currentRun, State state, String message, Exception exception) {
			_eventType = Objects.requireNonNull(eventType);
			_currentRun = Objects.requireNonNull(currentRun);
			_state = state;
			_message = message;
			_exception = exception;
		}
		
		public EventType getEventType() {
			return _eventType;
		}
		
		public TestRunInfo getTestRunInfo() {
			return _currentRun;
		}
		
		public State getState() {
			return _state;
		}
		
		public String getProgressMessage() {
			return _message;
		}
		
		public boolean hadException() {
			return _exception != null;
		}
		
		public Exception getException() {
			return _exception;
		}
		
	}
	
	public static interface TestTaskListener {
		void onTestTaskEvent(TestTask task, TestTaskEvent event);
	}
}
