package com.adamroughton.concentus.data.cluster.kryo;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestDependencyTree {

	@Test
	public void emptyTree() {
		DependencyTree tree = DependencyTree.newDependencyTree();
		for (String type : tree) {
			fail(String.format("Should be no elements (found %s)", type));
		}
	}
	
	@Test
	public void oneElement() {
		DependencyTree tree = DependencyTree.newDependencyTree();
		tree.dependsOn("Type1");
		
		String[] expected = new String[] { "Type1" };
		List<String> actualList = new ArrayList<>();
		for (String type : tree) {
			actualList.add(type);
		}
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void linearTree() {
		DependencyTree tree = DependencyTree.newDependencyTree();
		List<String> expectedList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			tree = tree.dependsOn("Type" + i);
			expectedList.add("Type" + (9 - i));
		}
		
		List<String> actualList = new ArrayList<>();
		for (String type : tree) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void oneLeftLinearRight() {
		DependencyTree root = DependencyTree.newDependencyTree();
		DependencyTree current = root;
		List<String> expectedList = new ArrayList<>();
		
		// left
		root.dependsOn("LeftType");
		expectedList.add("LeftType");
		
		// linear list right
		for (int i = 0; i < 10; i++) {
			current = current.dependsOn("Type" + i);
			expectedList.add("Type" + (9 - i));
		}
		
		List<String> actualList = new ArrayList<>();
		for (String type : root) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void fullTree() {
		DependencyTree root = DependencyTree.newDependencyTree();
		int depth = 5;
		int breadth = 10;
		
		List<String> expectedList = new ArrayList<>();
		createFullTree(0, depth, root, breadth, expectedList);
		
		List<String> actualList = new ArrayList<>();
		for (String type : root) {
			actualList.add(type);
		}
		
		String[] expected = expectedList.toArray(new String[0]);
		String[] actual= actualList.toArray(new String[0]);
		
		assertArrayEquals(expected, actual);
	}
	
	private static int createFullTree(int nextId, int depth, DependencyTree root, int breadth, List<String> expectedList) {
		if (depth > 1) {
			for (int i = 0; i < breadth; i++) {
				String type = "Type" + nextId;
				nextId = createFullTree(nextId + 1, depth - 1, root.dependsOn(type), breadth, expectedList);
				expectedList.add(type);
			}
		}
		return nextId;
	}
	
}
