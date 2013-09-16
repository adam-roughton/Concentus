package com.adamroughton.concentus.cluster;

public interface ClusterPath {

	String getRelativePath();
	
	String getAbsolutePath(String basePath);
	
	boolean isRelativeToRoot();
	
}
