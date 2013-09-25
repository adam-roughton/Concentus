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
	
	public static class TestTaskEvent {
		
		private final State _state;
		private final Exception _exception;
		
		public TestTaskEvent(State state, Exception exception) {
			_state = Objects.requireNonNull(state);
			_exception = exception;
		}
		
		public State getState() {
			return _state;
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
