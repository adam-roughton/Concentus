package com.adamroughton.concentus.cluster.coordinator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestServiceDependencySet {

	@Test
	public void emptySet() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		for (String type : dependencySet) {
			fail(String.format("Should be no elements (found %s)", type));
		}
	}
	
	@Test
	public void oneElementNoDepdencies() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		dependencySet.addDependencies("Type1");
		
		String[] expected = new String[] { "Type1" };
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void oneElementOneDependency() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		dependencySet.addDependencies("Type1", "Type2");
		
		String[] expected = new String[] { "Type2", "Type1" };
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void oneElementManyDependencies() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		dependencySet.addDependencies("Type1", "Type2", "Type3", "Type4");
		
		String[] expected = new String[] { "Type2", "Type3", "Type4", "Type1" };
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void linearChain() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		List<String> expectedList = new ArrayList<>();
		expectedList.add("RootType");
		String current = "RootType";
		for (int i = 0; i < 10; i++) {
			String next = "Type" + i;
			dependencySet.addDependency(current, next);
			current = next;
			expectedList.add(0, next);
		}
		
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void updateAlongTwoConvergentPaths() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		
		dependencySet.addDependencies("Type2", "Type3");
		dependencySet.addDependencies("Type1", "Type2", "Type3");
				
		List<String> expectedList = Arrays.asList("Type3", "Type2", "Type1");
		
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void repeatedDependency() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		
		dependencySet.addDependencies("Type2", "Type3");
		dependencySet.addDependencies("Type2", "Type3");
				
		List<String> expectedList = Arrays.asList("Type3", "Type2");
		
		List<String> actualList = new ArrayList<>();
		for (String type : dependencySet) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test(expected=IllegalStateException.class)
	public void cycle() {
		ServiceDependencySet dependencySet = new ServiceDependencySet();
		
		dependencySet.addDependencies("Type2", "Type3");
		dependencySet.addDependencies("Type3", "Type2");
	}
}
