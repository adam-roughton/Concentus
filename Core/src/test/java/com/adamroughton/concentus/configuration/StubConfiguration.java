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
package com.adamroughton.concentus.configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ServiceConfig;
import com.adamroughton.concentus.config.ZooKeeper;

public class StubConfiguration extends Configuration {

	private final StubService _stubService = new StubService();
	private final StubZooKeeper _stubZooKeeper = new StubZooKeeper();
	
	@Override
	public String getWorkingDir() {
		return "stub";
	}

	@Override
	public ZooKeeper getZooKeeper() {
		return _stubZooKeeper;
	}

	@Override
	public Map<String, ServiceConfig> getServices() {
		return new StubMap<ServiceConfig>(_stubService);
	}
	
	private static class StubService extends ServiceConfig {

		@Override
		public String getName() {
			return "stub";
		}

		@Override
		public Map<String, Integer> getPorts() {
			return new StubMap<Integer>(8080);
		}

		@Override
		public Map<String, Integer> getMessageBufferSizes() {
			return new StubMap<Integer>(2048);
		}
		
	}
	
	private static class StubZooKeeper extends ZooKeeper {

		@Override
		public String getAppRoot() {
			return "/stub";
		}
		
	}
	
	private static class StubMap<T> implements Map<String, T> {

		private final T _stubObject;
		
		public StubMap(T stubObject) {
			_stubObject = stubObject;
		}
		
		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public boolean containsKey(Object key) {
			return true;
		}

		@Override
		public boolean containsValue(Object value) {
			return _stubObject.equals(value);
		}

		@Override
		public T get(Object key) {
			return _stubObject;
		}

		@Override
		public T put(String key, T value) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public T remove(Object key) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public void putAll(Map<? extends String, ? extends T> m) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public Set<String> keySet() {
			return new StubKeySet();
		}

		@Override
		public Collection<T> values() {
			return Arrays.asList(_stubObject);
		}

		@Override
		public Set<java.util.Map.Entry<String, T>> entrySet() {
			throw new UnsupportedOperationException("Not implemented");
		}
		
	}
	
	private static class StubKeySet implements Set<String>{

		@Override
		public int size() {
			return 1;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public boolean contains(Object o) {
			return true;
		}

		@Override
		public Iterator<String> iterator() {
			return Arrays.asList("stub").iterator();
		}

		@Override
		public Object[] toArray() {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public <T> T[] toArray(T[] a) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean add(String e) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean remove(Object o) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean addAll(Collection<? extends String> c) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("Not implemented");
		}
		
	}
}
