package com.adamroughton.concentus.metric;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public final class MetricGroup {

	private final Set<Metric<?>> _children;
	
	public MetricGroup(Metric<?>... metrics) {
		this(Arrays.asList(metrics));
	}
	
	public MetricGroup(Collection<Metric<?>> metrics) {
		_children = new HashSet<>(metrics);
	}
	
	public long nextBucketReadyTime() {
		long minReadyTime = Long.MAX_VALUE;
		for (Metric<?> metric : _children) {
			minReadyTime = Math.min(minReadyTime, metric.nextBucketReadyTime());
		}
		return minReadyTime;
	}
	
	public long bucketIdLowerBound() {
		long lowerBound = Long.MAX_VALUE;
		for (Metric<?> metric : _children) {
			lowerBound = Math.min(lowerBound, metric.bucketIdLowerBound());
		}
		return lowerBound;
	}

	public void publishPending() {
		for (Metric<?> metric : _children) {
			metric.publishPending();
		}
	}
	
	public Set<Metric<?>> getChildren() {
		return _children;
	}
	
	public <T extends Metric<?>> T add(T metric) {
		_children.add(metric);
		return metric;
	}
	
	public boolean remove(Metric<?> metric) {
		return _children.remove(metric);
	}
	
}
