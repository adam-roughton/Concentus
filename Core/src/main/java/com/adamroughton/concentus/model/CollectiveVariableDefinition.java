package com.adamroughton.concentus.model;

public final class CollectiveVariableDefinition {

	private final int _variableId;
	private final int _topNCount;
	
	public CollectiveVariableDefinition(int variableId, int topNCount) {
		_variableId = variableId;
		_topNCount = topNCount;
	}
	
	public int getVariableId() {
		return _variableId;
	}
	
	public int getTopNCount() {
		return _topNCount;
	}
	
}
