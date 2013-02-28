package com.adamroughton.consentus.messaging;

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
