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
package com.adamroughton.concentus.cluster;

import java.util.Objects;
import com.netflix.curator.utils.ZKPaths;

public enum ClusterPath {
	APP_ROOT				(""),
	STATE					("state"),
	COORDINATOR				("coordinator"),
	ASSIGN_REQ				(STATE, "assignments/req"),
	ASSIGN_RES				(STATE, "assignments/res"),
	RUN_INFO				("currentRun"),
	RUN_CLIENT_COUNT		(RUN_INFO, "clientCount"),
	RUN_DURATION			(RUN_INFO, "duration"),
	RUN_WORKER_ALLOCATIONS 	(RUN_INFO, "workerAllocations"),
	APPLICATION   			("application"),
	ACTION_PROCESSOR_IDS	("actionProcessors"),
	SERVICES				("services"),
	READY					(STATE, "ready"),
	METRIC					("metric"),
	METRIC_PUBLISHERS 		(METRIC, "publishers");
	
	private final String _relativePathToRoot;
	
	private ClusterPath(String relativePathToRoot) {
		_relativePathToRoot = Objects.requireNonNull(relativePathToRoot);
	}
	
	private ClusterPath(ClusterPath basePath, String relativePath) {
		_relativePathToRoot = String.format("%s/%s", basePath.getRelativePath(), relativePath);
	}
	
	public String getRelativePath() {
		return _relativePathToRoot;
	}
	
	public String getPath(String root) {
		return ZKPaths.makePath(root, _relativePathToRoot);
	}
}
