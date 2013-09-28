package com.adamroughton.concentus.crowdhammer;

import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class LogBase2ClientCount implements IntIterable {

	private final int _start;
	private final int _count;
	
	public LogBase2ClientCount(int start, int count) {
		if (start != 0 && Integer.bitCount(start) != 1) {
			throw new IllegalArgumentException("The start count must be a power of 2! (was " + start + ")");
		}
		_start = start;
		_count = count;
	}
	
	@Override
	public IntIterator iterator() {
		return new IntIterator() {
			
			int index = 0;
			
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public Integer next() {
				return nextInt();
			}
			
			@Override
			public boolean hasNext() {
				return (index < _count);
			}
			
			@Override
			public int skip(int n) {
				index += n;
				return nextInt();
			}
			
			@Override
			public int nextInt() {
				if (index >= _count)
					throw new NoSuchElementException();
				
				if (_start == 0) {
					return 1 << (index++ + 1);
				} else {
					return _start << index++;
				}
			}
		};
	}

}
