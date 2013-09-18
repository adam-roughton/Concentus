package com.adamroughton.concentus.util;

/**
 * Wraps an object with a wrapper that ensures
 * that hash based collections will match the contained
 * object on object identity (rather than custom {@link Object#hashCode()}
 * or {@link Object#equals(Object)} implementations of the wrapped object).
 * 
 */
public final class IdentityWrapper<T> {

	private final T _obj;
	
	public IdentityWrapper(T obj) {
		_obj = obj;
	}

	@Override
	public int hashCode() {
		return System.identityHashCode(_obj);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof IdentityWrapper<?>) {
			IdentityWrapper<?> otherWrapper = (IdentityWrapper<?>) other;
			return this._obj == otherWrapper._obj;
		} else {
			return false;
		}
	}
	
	public T get() {
		return _obj;
	}
	
}
