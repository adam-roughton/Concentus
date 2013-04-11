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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.CapturesArguments;

/**
 * Current Mockito Argument Captors do not make a copy of the 
 * array on capture, only the reference to the array. In our case
 * we need to reuse the same array for zero copy purposes, which
 * requires that the array contents be copied for argument comparison.
 * This matcher fills the void by capturing and copying byte array
 * arguments.
 * 
 * @author Adam Roughton
 *
 */
public class ByteArrayCaptor extends ArgumentMatcher<byte[]> implements CapturesArguments {
	
	private final LinkedList<byte[]> _values = new LinkedList<>();
	
	@Override
	public boolean matches(Object argument) {
		return argument instanceof byte[];		
	}
	
	public List<byte[]> getAllValues() {
		LinkedList<byte[]> res = new LinkedList<>();
		for (byte[] content : _values) {
			res.add(Arrays.copyOf(content, content.length));
		}
		return res;
	}
	
	public byte[] getValue() {
		byte[] val = _values.getLast();
		return Arrays.copyOf(val, val.length);
	}

	@Override
	public void captureFrom(Object argument) {
		byte[] array = (byte[]) argument;
		_values.add(Arrays.copyOf(array, array.length));
	}

}
