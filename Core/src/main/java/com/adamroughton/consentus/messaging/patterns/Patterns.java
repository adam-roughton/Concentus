package com.adamroughton.consentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;

public class Patterns {
	
	public static MessageFrameBufferMapping validate(final MessageFrameBufferMapping mapping, final int expectedPartCount) {
		Objects.requireNonNull(mapping);
		int partCount = mapping.partCount();
		if (partCount != expectedPartCount) 
			throw new IllegalArgumentException(String.format("Expected %d message parts (got %d)", expectedPartCount, partCount));
		return mapping;
	}
	
}
