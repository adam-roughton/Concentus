package com.adamroughton.consentus;

import java.util.Objects;

import com.lmax.disruptor.ExceptionHandler;

public class FailFastExceptionHandler implements ExceptionHandler {

	private final String _disruptorName;
	private final FatalExceptionCallback _fatalExceptionCallback;
	
	public FailFastExceptionHandler(final String disruptorName, FatalExceptionCallback fatalExceptionCallback) {
		_disruptorName = disruptorName;
		_fatalExceptionCallback = Objects.requireNonNull(fatalExceptionCallback);
	}
	
	@Override
	public void handleEventException(Throwable ex, long sequence,
			Object event) {
		signalException(String.format("sequence %d", sequence), ex);
	}

	@Override
	public void handleOnStartException(Throwable ex) {
		signalException("start", ex);
	}

	@Override
	public void handleOnShutdownException(Throwable ex) {
		signalException("shutdown", ex);
	}
	
	private void signalException(final String location, final Throwable ex) {
		_fatalExceptionCallback.signalFatalException(
				new RuntimeException(String.format("Fatal exception on disruptor %s on %s",
						_disruptorName, location), ex));
	}
}