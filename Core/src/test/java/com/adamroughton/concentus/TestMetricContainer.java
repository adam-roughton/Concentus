package com.adamroughton.concentus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.MetricContainer.MetricLamda;

import uk.co.real_logic.intrinsics.ComponentFactory;

import static org.junit.Assert.*;
import static com.adamroughton.concentus.Constants.METRIC_TICK;

public class TestMetricContainer {

	private static final int WINDOW_SIZE = 8;
	
	private MetricContainer<MetricEntry> _container;
	
	@Before
	public void setUp() {
		_container = new MetricContainer<>(WINDOW_SIZE, new ComponentFactory<MetricEntry>() {

			@Override
			public MetricEntry newInstance(Object[] initArgs) {
				return new MetricEntry();
			}
		});
	}
	
	@Test
	public void testSingle() {
		List<Long> expected = Arrays.asList(5l);
		
		MetricEntry entry = _container.getMetricEntry();
		entry.metric1 = 5;
		
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);
	}
	
	@Test
	public void testSequentialLessThanWindow() {
		List<Long> expected = Arrays.asList(0l, 1l, 2l, 3l);
		
		long startTime = 235265243;
		int index = 0;
		for (long time = startTime; time < startTime + METRIC_TICK * WINDOW_SIZE / 2; time += METRIC_TICK) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSequentialGreaterThanWindow() {
		List<Long> expected = Arrays.asList(8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l);
		
		long startTime = 235265243;
		int index = 0;
		for (long time = startTime; time < startTime + METRIC_TICK * WINDOW_SIZE * 2; time += METRIC_TICK) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSequential_StepSmallerThanTick_GreaterThanWindow() {
		List<Long> expected = Arrays.asList(17l, 19l, 21l, 23l, 25l, 27l, 29l, 31l);
		
		long startTime = 235265243;
		int index = 0;
		for (long time = startTime; time < startTime + METRIC_TICK * WINDOW_SIZE * 2; time += METRIC_TICK / 2) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSkipSmallerThanWindow() {
		List<Long> expected = Arrays.asList(0l, 1l, 2l);
		long baseTime = 235265243;
		int index = 0;
		for (long time : Arrays.asList(baseTime, baseTime + METRIC_TICK * 5, baseTime + METRIC_TICK * 6)) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSkipWindowEdges() {
		List<Long> expected = Arrays.asList(0l, 1l);
		long baseTime = 235265243;
		int index = 0;
		for (long time : Arrays.asList(baseTime, baseTime + METRIC_TICK * (WINDOW_SIZE - 1))) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testSkipGreaterThanWindow() {
		List<Long> expected = Arrays.asList(1l, 2l);
		long baseTime = 235265243;
		int index = 0;
		for (long time : Arrays.asList(baseTime, baseTime + METRIC_TICK * 15, baseTime + METRIC_TICK * 19)) {
			long bucketId = time / METRIC_TICK;
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = index++;
		}
		
		final List<Long> actual = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(MetricEntry metricEntry) {
				actual.add(metricEntry.metric1);
			}
			
		});
		assertEquals(expected, actual);		
	}
	
	
	public static class MetricEntry {
		public long metric1;
	}
	
}
