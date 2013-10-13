package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class PerfTestSqliteMetricStore extends TestSqliteMetricStoreBase {

	@Test
	public void smallBatchSize() {
		double throughput = doTest(32, 1000);
		System.out.println("Had throughput " + throughput + " for batch size 32");
	}
	
	@Test
	public void largeBatchSize() {
		double throughput = doTest(2048, 1000);
		System.out.println("Had throughput " + throughput + " for batch size 2048");
	}
	
	
	private double doTest(int batchSize, int batchCount) {
		long startTime = System.currentTimeMillis();
		long bucketId = 1000;
		for (int i = 0; i < batchCount; i++) {
			for (int j = 0; j < batchSize; j++) {
				_metricStore.pushCountMetric(0, 23, 1, bucketId++, 1000, 48);
			}
			_metricStore.onEndOfBatch();
		}
		long duration = System.currentTimeMillis() - startTime;
		return batchSize * batchCount / (double) duration * TimeUnit.SECONDS.toMillis(1);
	}
	
	
}
