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
package com.adamroughton.consentus.messaging;

import java.nio.ByteBuffer;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ZmqTestUtil {

	public static Answer<Integer> fakeRecv(final ByteBuffer content) {
		return fakeRecv(content.array());
	}
	
	public static Answer<Integer> fakeRecv(final byte[] content) {
		return new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				Object[] args = invocation.getArguments();
				byte[] array = (byte[]) args[0];
				int offset = (int) args[1];
				int bufferLength = (int) args[2];
				int length = content.length > bufferLength? bufferLength : content.length;
				System.arraycopy(content, 0, array, offset, length);
				return length;
			}
		};
	}
	
	public static Integer[] repeatReturnVal(int val, int times) {
		Integer[] rVals = new Integer[times];
		for (int i = 0; i < rVals.length; i++) {
			rVals[i] = val;
		}
		return rVals;
	}
	
	public static Answer<Integer> fakeBlockingRecv(final BlockingCall blockingCall) {
		return new Answer<Integer>() {
			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				blockingCall.block();
				return -1;
			}
		};
	}
	
	public static <T> ArrayLengthMatcher<T> matchesLength(final T array) {
		int length = java.lang.reflect.Array.getLength(array);
		return new ArrayLengthMatcher<>(length);
	}
	
	public static ByteArrayMatcher hasBytes(final byte[] bytes) {
		return new ByteArrayMatcher(bytes);
	}
	
	public static ByteArrayMatcher hasBytesInRange(final byte[] bytes, int offsetInActual, int length) {
		return new ByteArrayMatcher(bytes, offsetInActual, length);
	}
	
	public static boolean arrayRangeEqual(byte[] expected, byte[] actual, int offsetOnActual, int length) {
		if (offsetOnActual < 0)
			throw new IllegalArgumentException("The offset must be 0 or greater.");
		if (length < 0)
			throw new IllegalArgumentException("The length must be 0 or greater.");
		
		if (actual.length < offsetOnActual + length) {
			return false;
		}
		for (int i = 0; i < expected.length; i++) {
			if (expected[i] != actual[i + offsetOnActual])
				return false;
		}
		return true;
	}
	
}
