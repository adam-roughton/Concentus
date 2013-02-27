package com.adamroughton.consentus.messaging;

import org.mockito.ArgumentMatcher;

public class ByteArrayMatcher extends ArgumentMatcher<byte[]> {

	private final byte[] _expected;
	private final int _offsetOnActual;
	private final int _length;
	
	public ByteArrayMatcher(final byte[] expected) {
		this(expected, 0, expected.length);
	}
	
	public ByteArrayMatcher(final byte[] expected, int offsetOnActual, int length) {
		if (offsetOnActual < 0)
			throw new IllegalArgumentException("The offset must be 0 or greater.");
		if (length < 0)
			throw new IllegalArgumentException("The length must be 0 or greater.");
		
		_expected = expected;
		_offsetOnActual = offsetOnActual;
		_length = length;
	}
	
	@Override
	public boolean matches(Object argument) {
		if (!(argument instanceof byte[]))
			return false;
		
		byte[] actual = (byte[]) argument;
		if (actual.length < _offsetOnActual + _length) {
			return false;
		}
		for (int i = 0; i < _expected.length; i++) {
			if (_expected[i] != actual[i + _offsetOnActual])
				return false;
		}
		return true;
	}

}
