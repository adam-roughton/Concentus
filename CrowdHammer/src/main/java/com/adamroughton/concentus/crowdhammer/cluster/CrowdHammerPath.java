package com.adamroughton.concentus.crowdhammer.cluster;

import java.util.Objects;

import com.adamroughton.concentus.cluster.ClusterPath;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.CorePath.*;

public enum CrowdHammerPath implements ClusterPath {
	
	TEST_ROOT				(APP_ROOT, "crowdhammer"),

	CLIENT_AGENT			(TEST_ROOT, "clientAgent"),	
	;
	
	private final String _relativePath;
	private final boolean _isRelativeToRoot;
	
	private CrowdHammerPath(String relativePath, boolean isRelativeToRoot) {
		_relativePath = Objects.requireNonNull(relativePath);
		_isRelativeToRoot = isRelativeToRoot;
	}
	
	private CrowdHammerPath(ClusterPath parentPath, String relativePath) {
		_relativePath = ZKPaths.makePath(parentPath.getRelativePath(), relativePath);
		_isRelativeToRoot = parentPath.isRelativeToRoot();
	}
	
	public String getRelativePath() {
		return _relativePath;
	}
	
	public String getAbsolutePath(String basePath) {
		return ZKPaths.makePath(basePath, _relativePath);
	}
	
	public boolean isRelativeToRoot() {
		return _isRelativeToRoot;
	}
}
