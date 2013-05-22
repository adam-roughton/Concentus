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
package com.adamroughton.concentus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.MetricContainer.MetricLamda;
import com.adamroughton.concentus.InitialiseDelegate;

import uk.co.real_logic.intrinsics.ComponentFactory;

import static org.junit.Assert.*;

public class TestMetricContainer {

	private static final int WINDOW_SIZE = 8;
	
	private MetricContainer<MetricEntry> _container;
	private DrivableClock _clock;
	
	@Before
	public void setUp() {
		_clock = new DrivableClock();
		_clock.setTime(235262444, TimeUnit.MILLISECONDS);
		
		_container = new MetricContainer<>(
			_clock,
			WINDOW_SIZE, 
			MetricEntry.class,
			new ComponentFactory<MetricEntry>() {

				@Override
				public MetricEntry newInstance(Object[] initArgs) {
					return new MetricEntry();
				}
			},
			new InitialiseDelegate<MetricEntry>() {
	
				@Override
				public void initialise(MetricEntry content) {
					content.metric1 = 0;
				}
				
			});
	}
	
	@Test
	public void single() {
		doSetAndGetPendingTest(
				usingBucketIds(0), 
				expectingPendingBucketIds(0));
	}
	
	@Test
	public void sequentialLessThanWindow() {
		doSetAndGetPendingTest(
				usingBucketIdsBetween(0, WINDOW_SIZE / 2),
				expectedPendingBucketIdsBetween(0, WINDOW_SIZE / 2));	
	}
	
	@Test
	public void sequentialGreaterThanWindow() {
		doSetAndGetPendingTest(
				usingBucketIdsBetween(0, WINDOW_SIZE * 2 - 1),
				expectedPendingBucketIdsBetween(WINDOW_SIZE, WINDOW_SIZE * 2 - 1));		
	}
	
	@Test
	public void sequential_StepSmallerThanTick_GreaterThanWindow() {
		doSetAndGetPendingTest(
				usingBucketIds(0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5), 
				expectingPendingBucketIds(0, 1, 2, 3, 4, 5));		
	}
	
	@Test
	public void skipSmallerThanWindow() {
		doSetAndGetPendingTest(
				usingBucketIds(0, 5, 6), 
				expectingPendingBucketIds(0, 5, 6));		
	}
	
	@Test
	public void skipWindowEdges() {
		doSetAndGetPendingTest(
				usingBucketIds(0, (WINDOW_SIZE - 1)), 
				expectingPendingBucketIds(0, (WINDOW_SIZE - 1)));
	}
	
	@Test
	public void skipGreaterThanWindow() {
		doSetAndGetPendingTest(
				usingBucketIds(0, 15, 19),
				expectingPendingBucketIds(15, 19));	
	}
	
	@Test
	public void currentNotInPendingSet_CentreOfBucket() throws InterruptedException {
		long centredBucketTime = (Constants.METRIC_TICK * 20) / 15;
		_clock.setTime(centredBucketTime, TimeUnit.MILLISECONDS);
		long currentBucketId = _container.getCurrentMetricBucketId();
		doSetAndGetPendingTest(
				usingBucketIds(currentBucketId - 4, currentBucketId - 1, currentBucketId), 
				expectingPendingBucketIds(currentBucketId - 4, currentBucketId - 1));
	}
	
	@Test
	public void currentNotInPendingSet_StartOfBucket() throws InterruptedException {
		long startBucketTime = Constants.METRIC_TICK * 20000;
		_clock.setTime(startBucketTime, TimeUnit.MILLISECONDS);
		long currentBucketId = _container.getCurrentMetricBucketId();
		doSetAndGetPendingTest(
				usingBucketIds(currentBucketId - 4, currentBucketId - 1, currentBucketId), 
				expectingPendingBucketIds(currentBucketId - 4, currentBucketId - 1));
	}
	
	@Test
	public void currentNotInPendingSet_endOfBucket() throws InterruptedException {
		long endBucketTime = Constants.METRIC_TICK * 20000 - 1;
		_clock.setTime(endBucketTime, TimeUnit.MILLISECONDS);
		long currentBucketId = _container.getCurrentMetricBucketId();
		doSetAndGetPendingTest(
				usingBucketIds(currentBucketId - 4, currentBucketId - 1, currentBucketId), 
				expectingPendingBucketIds(currentBucketId - 4, currentBucketId - 1));
	}
	
	private void doSetAndGetPendingTest(List<Long> bucketIds, List<Long> expectedPendingIds) {
		for (long bucketId : bucketIds) {
			MetricEntry entry = _container.getMetricEntry(bucketId);
			entry.metric1 = bucketId;
		}
		
		final List<Long> actualPendingIds = new ArrayList<>();
		_container.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(long bucketId, MetricEntry metricEntry) {
				assertEquals(bucketId, metricEntry.metric1);
				actualPendingIds.add(bucketId);
			}
			
		});
		assertEquals(expectedPendingIds, actualPendingIds);
	}
	
	private List<Long> usingBucketIdsBetween(long first, long last) {
		return range(first, last);
	}
	
	private List<Long> usingBucketIds(long... ids) {
		return arrayToList(ids);
	}
	
	private List<Long> expectedPendingBucketIdsBetween(long first, long last) {
		return range(first, last);
	}
	
	private List<Long> expectingPendingBucketIds(long... expectedValues) {
		return arrayToList(expectedValues);
	}
	
	private List<Long> arrayToList(long[] array) {
		List<Long> list = new ArrayList<>(array.length);
		for (long element : array) {
			list.add(element);
		}
		return list;
	}
	
	private List<Long> range(Long first, Long last) {
		List<Long> rangeList = new ArrayList<>();
		for (Long id = first; id <= last; id++) {
			rangeList.add(id);
		}
		return rangeList;
	}
	
	public static class MetricEntry {
		public long metric1;
	}
	
}
