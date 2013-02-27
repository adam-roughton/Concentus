package com.adamroughton.consentus.messaging;

import org.mockito.ArgumentMatcher;

public class ArrayLengthMatcher<T> extends ArgumentMatcher<T> {

	private final int _expectedLength;
	
	public ArrayLengthMatcher(final int expectedLength) {
		_expectedLength = expectedLength;
	}
	
	@Override
	public boolean matches(Object argument) {
		int length = java.lang.reflect.Array.getLength(argument);
		return length == _expectedLength;
	}
}