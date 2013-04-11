/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.messaging;

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
