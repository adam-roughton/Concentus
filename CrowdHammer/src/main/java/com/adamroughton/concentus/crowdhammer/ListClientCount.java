package com.adamroughton.concentus.crowdhammer;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;

public class ListClientCount implements IntIterable {

	private final IntArrayList _clientCounts;
	
	public ListClientCount(int firstClientCount, int...additionalClientCounts) {
		_clientCounts = new IntArrayList(additionalClientCounts.length + 1);
		_clientCounts.add(firstClientCount);
		for (int additional : additionalClientCounts) {
			_clientCounts.add(additional);
		}
	}
	
	@Override
	public IntIterator iterator() {
		return _clientCounts.iterator();
	}
	
}
