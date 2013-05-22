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
package com.adamroughton.concentus.cluster.coordinator;

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