package com.adamroughton.consentus.cluster.coordinator;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Allows a snapshot to be made of the participating nodes
 * that will be used during the execution phases.
 * 
 * @author Adam Roughton
 *
 */
public class ParticipatingNodes {
	
	private final Set<UUID> _set;
	
	public static ParticipatingNodes create() {
		return new ParticipatingNodes(new HashSet<UUID>());
	}
	
	private ParticipatingNodes(final Set<UUID> set) {
		_set = set;
	}
	
	public ParticipatingNodes add(final UUID nodeId) {
		Objects.requireNonNull(nodeId);
		Set<UUID> newSet = new HashSet<>(_set);
		newSet.add(nodeId);
		return new ParticipatingNodes(newSet);
	}
	
	public ParticipatingNodes add(final Iterable<UUID> nodeIds) {
		ParticipatingNodes participatingNodes = this;
		for (UUID nodeId : nodeIds) {
			participatingNodes = participatingNodes.add(nodeId);
		}
		return participatingNodes;
	}
	
	public int getCount() {
		return _set.size();
	}
	
	public ParticipatingNodes.ParticipatingNodesLatch createNodesLatch() {
		return new ParticipatingNodesLatch(new HashSet<>(_set));
	}
	
	/**
	 * Allows the participating nodes to be tracked as they become ready.
	 * 
	 * @author Adam Roughton
	 *
	 */
	public static class ParticipatingNodesLatch {
		
		private final Set<UUID> _set;
		
		private ParticipatingNodesLatch(final Set<UUID> set) {
			_set = Objects.requireNonNull(set);
		}
		
		public void accountFor(final UUID nodeId) {
			_set.remove(nodeId);
		}
		
		public int remainingCount() {
			return _set.size();
		}
		
		/**
		 * Checks if all of the nodes have been accounted for.
		 * @return {@code true} if all nodes have been accounted for, {@code false} otherwise
		 */
		public boolean isDone() throws IllegalStateException {
			return _set.size() == 0;
		}
		
	}
	
}