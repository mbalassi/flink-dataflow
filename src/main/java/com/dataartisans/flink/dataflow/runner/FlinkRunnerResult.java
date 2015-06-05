/*
 * Copyright 2015 Data Artisans GmbH
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
package com.dataartisans.flink.dataflow.runner;

import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.runners.AggregatorRetrievalException;
import com.google.cloud.dataflow.sdk.runners.AggregatorValues;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;

import java.util.Collections;
import java.util.Map;

/**
 * Result of executing a {@link com.google.cloud.dataflow.sdk.Pipeline} with Flink. This
 * has methods to query to job runtime and the final values of
 * {@link com.google.cloud.dataflow.sdk.transforms.Aggregator}s.
 */
public class FlinkRunnerResult implements PipelineResult {
	
	private final Map<String, Object> aggregators;
	
	private final long runtime;
	
	public FlinkRunnerResult(Map<String, Object> aggregators, long runtime) {
		this.aggregators = (aggregators == null || aggregators.isEmpty()) ?
				Collections.<String, Object>emptyMap() :
				Collections.unmodifiableMap(aggregators);
		
		this.runtime = runtime;
	}

	/**
	 * Return the final values of all {@link com.google.cloud.dataflow.sdk.transforms.Aggregator}s
	 * used in the {@link com.google.cloud.dataflow.sdk.Pipeline}.
	 */
	//TODO fix me
	@Override
	public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
//		if (aggregators.containsKey(aggregator.getName())){
//			return (Aggregator<?, T>) aggregators.get(aggregator.getName());
//		}
		return null;
	}

	public Map<String, Object> getAggregators() {
		return aggregators;
	}

	public long getRuntime() {
		return runtime;
	}

	@Override
	public State getState() {
		return null;
	}

}
