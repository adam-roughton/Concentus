package com.adamroughton.consentus.cluster;

import java.util.Objects;
import com.netflix.curator.utils.ZKPaths;

public enum ClusterPath {
	APP_ROOT	(""),
	STATE		("state"),
	COORDINATOR	("coordinator"),
	ASSIGN_REQ	(STATE, "assignments/req"),
	ASSIGN_RES	(STATE, "assignments/res"),
	SERVICES	("services"),
	READY		(STATE, "ready");
	
	private final String _relativePathToRoot;
	
	private ClusterPath(String relativePathToRoot) {
		_relativePathToRoot = Objects.requireNonNull(relativePathToRoot);
	}
	
	private ClusterPath(ClusterPath basePath, String relativePath) {
		_relativePathToRoot = String.format("%s/%s", basePath.getRelativePath(), "assignments/req");
	}
	
	public String getRelativePath() {
		return _relativePathToRoot;
	}
	
	public String getPath(String root) {
		return ZKPaths.makePath(root, _relativePathToRoot);
	}
}
