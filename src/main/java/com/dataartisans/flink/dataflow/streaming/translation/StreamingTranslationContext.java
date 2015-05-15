/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.streaming.translation;

import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface StreamingTranslationContext {

	public StreamExecutionEnvironment getExecutionEnvironment();

	public PipelineOptions getPipelineOptions();
	
	public <T> DataStream<T> getInputDataStream(PInput value);

	public <T> WindowedDataStream<T> getInputWindowedDataStream(PInput value);

	public void setOutputDataStream(POutput value, DataStream<?> stream);

	public void setOutputWindowedDataStream(POutput value, WindowedDataStream<?> stream);

	public <T> TypeInformation<T> getTypeInfo(POutput output);

	public PInput getInput(PTransform<?, ?> transform);

	public POutput getOutput(PTransform<?, ?> transform);

	public CoderRegistry getCoderRegistry(PTransform<?, ?> transform);
}
