package com.adamroughton.concentus.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;

class PipelineTopology<TEvent> implements Iterable<Collection<PipelineSegment<TEvent>>> {

	private final List<Collection<PipelineSegment<TEvent>>> _pipeline;
	
	public PipelineTopology() {
		_pipeline = Collections.emptyList();
	}
	
	private PipelineTopology(List<Collection<PipelineSegment<TEvent>>> pipeline) {
		_pipeline = pipeline;
	}
	
	public PipelineTopology<TEvent> add(PipelineSegment<TEvent> segment) {
		return add(_pipeline.size(), segment);
	}
	
	public PipelineTopology<TEvent> add(Collection<PipelineSegment<TEvent>> segments) {
		return add(_pipeline.size(), segments);
	}
	
	public PipelineTopology<TEvent> add(int layerIndex, Collection<PipelineSegment<TEvent>> segments) {
		List<Collection<PipelineSegment<TEvent>>> pipeline = deepMutableCopy(_pipeline);
		addInternal(layerIndex, segments, pipeline);
		return new PipelineTopology<>(pipeline);
	}
	
	public PipelineTopology<TEvent> add(int layerIndex, PipelineSegment<TEvent> segment) {
		return add(layerIndex, Arrays.asList(segment));
	}
	
	public Set<PipelineSegment<TEvent>> getLayer(int layerIndex) {
		return new HashSet<>(_pipeline.get(layerIndex));
	}
	
	public PipelineTopology<TEvent> combineWith(PipelineTopology<TEvent> topology, int layerIndex) {
		List<Collection<PipelineSegment<TEvent>>> pipeline = deepMutableCopy(_pipeline);
		for (Collection<PipelineSegment<TEvent>> layer : topology) {
			addInternal(layerIndex++, layer, pipeline);
		}
		return new PipelineTopology<>(pipeline);
	}

	@Override
	public Iterator<Collection<PipelineSegment<TEvent>>> iterator() {
		return new ArrayList<>(_pipeline).iterator();
	}
	
	public ListIterator<Collection<PipelineSegment<TEvent>>> listIterator() {
		return _pipeline.listIterator();
	}
	
	public ListIterator<Collection<PipelineSegment<TEvent>>> listIterator(int index) {
		return _pipeline.listIterator(index);
	}

	public int size() {
		return _pipeline.size();
	}
	
	private static <TEvent> void addInternal(int layerIndex, Collection<PipelineSegment<TEvent>> segments, List<Collection<PipelineSegment<TEvent>>> pipeline) {
		Objects.requireNonNull(segments);
		if (layerIndex >= pipeline.size()) {
			for (int i = pipeline.size(); i <= layerIndex; i++) {
				pipeline.add(i, new HashSet<PipelineSegment<TEvent>>());
			}
		}
		for (PipelineSegment<TEvent> segment : segments) {
			pipeline.get(layerIndex).add(segment);
		}
	}
	
	private static <TEvent> List<Collection<PipelineSegment<TEvent>>> deepMutableCopy(List<Collection<PipelineSegment<TEvent>>> pipeline) {
		ArrayList<Collection<PipelineSegment<TEvent>>> pipelineCopy = new ArrayList<>(pipeline.size());
		for (Collection<PipelineSegment<TEvent>> layer : pipeline) {
			// the segments are immutable, so no need to clone
			HashSet<PipelineSegment<TEvent>> copiedLayer = new HashSet<>(layer);
			pipelineCopy.add(copiedLayer);
		}
		return pipelineCopy;
	}
	
}
