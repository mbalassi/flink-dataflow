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
package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public interface TranslationContext {

	ExecutionEnvironment getExecutionEnvironment();

	PipelineOptions getPipelineOptions();
	
	<T> DataSet<T> getInputDataSet(PInput value);
	
	void setOutputDataSet(POutput value, DataSet<?> set);

	<T> DataSet<T> getSideInputDataSet(PCollectionView<?> value);

	void setSideInputDataSet(PCollectionView<?> value, DataSet<?> set);
	
	<T> TypeInformation<T> getTypeInfo(POutput output);

	<InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform);

	<OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform);

	CoderRegistry getCoderRegistry(PTransform<?, ?> transform);
}
