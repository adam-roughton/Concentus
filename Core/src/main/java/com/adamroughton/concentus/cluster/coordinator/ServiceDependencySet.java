package com.adamroughton.concentus.cluster.coordinator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ServiceDependencySet implements Iterable<String> {
	
	private final Map<String, ServiceDefinition> _serviceDefinitions = new HashMap<>();
	
	public void addDependency(String serviceType, String dependency) {
		addDependencies(serviceType, dependency);
	}
	
	public void addDependencies(String serviceType, String... dependencies) {
		// Create an item to track the dependency position for this service
		ServiceDefinition definition;
		if (_serviceDefinitions.containsKey(serviceType)) {
			definition = _serviceDefinitions.get(serviceType);
			definition.addDependencies(dependencies);
		} else {
			definition = new ServiceDefinition(serviceType, dependencies);
			_serviceDefinitions.put(serviceType, definition);
		}
		
		// update the dependencies
		for (String dependencyType : definition.dependencies) {
			updateDependency(dependencyType, definition.score, new String[] { serviceType });
		}
	}
	
	private void updateDependency(String serviceType, int scoreOfDependant, String[] dependencyChain) {
		ServiceDefinition definition;
		if (_serviceDefinitions.containsKey(serviceType)) {
			definition = _serviceDefinitions.get(serviceType);
		} else {
			definition = new ServiceDefinition(serviceType);
			_serviceDefinitions.put(serviceType, definition);
		}
		
		// inefficient, but sufficient for our purposes
		for (String dependency : dependencyChain) {
			if (dependency.equals(serviceType)) {
				throw new IllegalStateException("Cyclic dependency detected for '" + serviceType + "': " + 
						prettyPrintChain(serviceType, dependencyChain));
			}
		}

		definition.score = Math.max(definition.score, scoreOfDependant + 1);
		
		String[] newDependencyChain = Arrays.copyOf(dependencyChain, dependencyChain.length + 1);
		newDependencyChain[dependencyChain.length] = serviceType;
		
		// update the dependencies
		for (String dependencyType : definition.dependencies) {
			updateDependency(dependencyType, definition.score, newDependencyChain);
		}
	}
	
	@Override
	public Iterator<String> iterator() {
		List<ServiceDefinition> definitions = new ArrayList<>(_serviceDefinitions.values());
		Collections.sort(definitions);
		final Iterator<ServiceDefinition> definitionIterator = definitions.iterator();
		
		return new Iterator<String>() {

			@Override
			public boolean hasNext() {
				return definitionIterator.hasNext();
			}

			@Override
			public String next() {
				return definitionIterator.next().serviceType;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public Iterable<String> inReverse() {
		List<ServiceDefinition> definitions = new ArrayList<>(_serviceDefinitions.values());
		Collections.sort(definitions);
		final ListIterator<ServiceDefinition> definitionIterator = definitions.listIterator(definitions.size());
		
		return new Iterable<String>() {
			
			@Override
			public Iterator<String> iterator() {
				return new Iterator<String>() {

					@Override
					public boolean hasNext() {
						return definitionIterator.hasPrevious();
					}

					@Override
					public String next() {
						return definitionIterator.previous().serviceType;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}
	
	private static String prettyPrintChain(String head, String...dependencies) {
		StringBuilder strBuilder = new StringBuilder();
		for (String dependency : dependencies) {
			strBuilder.append(dependency);
			strBuilder.append(" -> ");
		}
		strBuilder.append(head);
		return strBuilder.toString();
	}
	
	private static class ServiceDefinition implements Comparable<ServiceDefinition> {
		public final String serviceType;
		public final Set<String> dependencies;
		public int score;
		
		public ServiceDefinition(String serviceType, String... dependencies) {
			this.serviceType = serviceType;
			this.dependencies = new HashSet<>();
			addDependencies(dependencies);
			this.score = 0;
		}
		
		public void addDependencies(String... dependencies) {
			for (String dependency : dependencies) {
				this.dependencies.add(dependency);
			}
		}
		
		@Override
		public int compareTo(ServiceDefinition other) {
			if (this.score != other.score) {
				return other.score - this.score;
			} else {
				return this.serviceType.compareTo(other.serviceType);
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + score;
			result = prime * result + Objects.hashCode(serviceType);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof ServiceDefinition)) {
				return false;
			}
			ServiceDefinition other = (ServiceDefinition) obj;
			if (this.score != other.score) {
				return false;
			} else if (!Objects.equals(this.serviceType, other.serviceType)) {
				return false;
			} else {
				return true;
			}
		}
	}

}
