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

	public ExecutionEnvironment getExecutionEnvironment();

	public PipelineOptions getPipelineOptions();
	
	public <T> DataSet<T> getInputDataSet(PInput value);
	
	public void setOutputDataSet(POutput value, DataSet<?> set);

	public <T> DataSet<T> getSideInputDataSet(PCollectionView<?> value);

	public void setSideInputDataSet(PCollectionView<?> value, DataSet<?> set);
	
	public <T> TypeInformation<T> getTypeInfo(POutput output);

	public PInput getInput(PTransform<?, ?> transform);

	public POutput getOutput(PTransform<?, ?> transform);

	public CoderRegistry getCoderRegistry(PTransform<?, ?> transform);
}
