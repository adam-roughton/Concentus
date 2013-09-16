package com.adamroughton.concentus.util;

public final class Container<T> {

	private T _item;
	
	public T get() {
		return _item;
	}
	
	public void set(T item) {
		_item = item;
	}
	
}
