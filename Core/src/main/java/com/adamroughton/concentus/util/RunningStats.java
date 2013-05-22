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
package com.adamroughton.concentus.util;

/**
 * Calculates the mean and variance of input data using
 * the online algorithm developed by Welford (1962) and presented
 * by Knuth in TAOCP vol 2, 3rd edition, pg 232.
 * Adapted from http://www.johndcook.com/standard_deviation.html
 * 
 * @author Adam Roughton
 *
 */
public class RunningStats {

	private int _count = 0;
	private double _mean, _sumSqrs;
	private double _min = 0, _max = 0;
	
	public RunningStats() {
	}
	
	public RunningStats(int count, double mean, double sumSqrs, double min, double max) {
		_count = count;
		_mean = mean;
		_sumSqrs = sumSqrs;
		_min = min;
		_max = max;
	}
	
	public void push(double value) {
		_count++;
		if (_count == 1) {
			_mean = _min = _max = value;
            _sumSqrs = 0.0;
		} else {
			double oldMean = _mean;
			double oldSumSqrs = _sumSqrs;
			
			_mean = oldMean + (value - oldMean)/_count;
            _sumSqrs = oldSumSqrs + (value - oldMean)*(value - _mean);
            
            _min = (value < _min)? value : _min;
            _max = (value > _max)? value : _max;
		}
	}
	
	public double getMean() {
		return _mean;
	}
	
	public double getSumOfSquares() {
		return _sumSqrs;
	}
	
	public double getVariance() {
		if (_count > 1) {
			return _sumSqrs / (_count - 1);
		} else {
			return 0.0;
		}
	}
	
	public double getStandardDeviation() {
		return Math.sqrt(getVariance());
	}
	
	public int getCount() {
		return _count;
	}
	
	public double getMin() {
		return _min;
	}
	
	public double getMax() {
		return _max;
	}
	
	public void merge(RunningStats stats) {
		merge(stats.getCount(), stats.getMean(), stats.getSumOfSquares(), stats.getMin(), stats.getMax());
	}
	
	public void merge(int count, double mean, double sumSqrs, double min, double max) {
		if (count > 0) {
			double meanDiff = _mean - mean;
			int newN = count + _count;
			_mean = (_count * _mean + count * mean) / (newN);
			_sumSqrs += sumSqrs + (meanDiff * meanDiff) * (_count * count / newN);
			_min = (min < _min)? min : _min;
			_max = (max > _max)? max : _max;
			_count = newN;
		}
	}
	
	public void reset() {
		_count = 0;
		_mean = 0;
		_sumSqrs = 0;
		_min = 0;
		_max = 0;
	}
	
}
