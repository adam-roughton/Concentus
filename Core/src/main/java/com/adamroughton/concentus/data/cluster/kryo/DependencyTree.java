package com.adamroughton.concentus.data.cluster.kryo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class DependencyTree implements Iterable<String> {

	private DependencyTree _parent;
	private List<DependencyTree> _children;
	private String _type;
	private Set<String> _seenSet;
	
	// for kryo
	private DependencyTree() {}
	
	private DependencyTree(DependencyTree parent, String type, Set<String> seenSet) {
		_parent = parent;
		_children = new ArrayList<>();
		_type = type;
		_seenSet = seenSet;
	}
	
	public static DependencyTree newDependencyTree() {
		return new DependencyTree(null, null, new HashSet<String>());
	}
	
	public boolean isRoot() {
		return _parent == null;
	}
	
	public DependencyTree dependsOn(String type) {
		if (_seenSet.contains(type)) {
			throw new IllegalArgumentException(String.format("Cannot create cyclic dependencies (attempted to add %s twice)", type));
		}
		_seenSet.add(type);
		DependencyTree child = new DependencyTree(this, type, _seenSet);
		_children.add(child);
		return child;
	}
	
	public DependencyTree getRoot() {
		DependencyTree tree = this;
		while (!tree.isRoot()) {
			tree = tree._parent;
		}
		return tree;
	}
	
	public String getType() {
		return _type;
	}
	
	@Override
	public String toString() {
		if (isRoot()) {
			return "Root";	
		} else {
			return _type;			
		}
	}
	
	@Override
	public Iterator<String> iterator() {
		return new DependencyTreeIterator(this);
	}
	
	private static class DependencyTreeIterator implements Iterator<String> {
		
		private DependencyTree _current;
		private Set<String> _closedSet;
		
		private String _next;
		
		public DependencyTreeIterator(DependencyTree tree) {
			_current = tree.getRoot();
			_closedSet = new HashSet<String>();
		}
		
		private DependencyTree depthFirstSearch(DependencyTree tree, Set<String> closedSet) {		
			for (DependencyTree child : tree._children) {
				if (!closedSet.contains(child.getType())) {
					return depthFirstSearch(child, closedSet);
				}
			}
			if (!closedSet.contains(tree.getType())) {
				return tree;
			} else if (tree.isRoot()) {
				return null;
			} else {
				return depthFirstSearch(tree._parent, closedSet);
			}
		}
		
		@Override
		public boolean hasNext() {
			if (_next != null) {
				return true;
			}
			
			_current = depthFirstSearch(_current, _closedSet);
			if (_current == null) {
				_next = null;
				return false;
			} else if (_current.isRoot()) {
				return false;
			} else {
				_next = _current.getType();
				_closedSet.add(_next);
				return true;
			}
		}

		@Override
		public String next() {
			if (_next != null) {
				String next = _next;
				_next = null;
				return next;
			} else {
				throw new IllegalStateException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
}
