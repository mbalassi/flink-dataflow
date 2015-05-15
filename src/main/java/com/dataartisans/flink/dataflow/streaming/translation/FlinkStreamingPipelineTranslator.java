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

import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.types.KvCoderTypeInformation;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * FlinkPipelineTranslator knows how to translate Pipeline objects into Flink Jobs.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator}
 */
public class FlinkStreamingPipelineTranslator implements PipelineVisitor, StreamingTranslationContext {

	private int depth = 0;

	private boolean inComposite = false;

	private final Map<POutput, DataStream<?>> dataStreams;
	private final Map<POutput, WindowedDataStream<?>> windowedDataStreams;

	private final StreamExecutionEnvironment env;
	private final PipelineOptions options;

	private Pipeline pipeline;

	private AppliedPTransform<?, ?, ?> currentTransform;

	public FlinkStreamingPipelineTranslator(StreamExecutionEnvironment env, PipelineOptions options) {
		this.env = env;
		this.options = options;
		this.dataStreams = new HashMap<>();
		this.windowedDataStreams = new HashMap<>();
	}

	public void translate(Pipeline pipeline) {
		this.pipeline = pipeline;
		pipeline.traverseTopologically(this);
	}


	// --------------------------------------------------------------------------------------------
	//  Pipeline Visitor Methods
	// --------------------------------------------------------------------------------------------

	private static String genSpaces(int n) {
		String s = "";
		for(int i = 0; i < n; i++) {
			s += "|   ";
		}
		return s;
	}

	private static String formatNodeName(TransformTreeNode node) {
		return node.toString().split("@")[1] + node.getTransform();
	}

	@Override
	public void enterCompositeTransform(TransformTreeNode node) {
		System.out.println(genSpaces(this.depth) + "enterCompositeTransform- " + formatNodeName(node));
		PTransform<?, ?> transform = node.getTransform();
		currentTransform = AppliedPTransform.of(node.getInput(), node.getOutput(), (PTransform<PInput, POutput>) node.getTransform());

		if (transform != null) {
			TransformTranslator<?> translator = FlinkStreamingTransformTranslators.getTranslator(transform);

			if (translator != null) {
				inComposite = true;

//				if (transform instanceof CoGroupByKey &&
//						((CoGroupByKey<?>) transform).getInput().getKeyedCollections().size() != 2) {
//					// we can only optimize CoGroupByKey for input size 2
//					inComposite = false;
//				}
			}
		}

		this.depth++;
	}

	@Override
	public void leaveCompositeTransform(TransformTreeNode node) {
		PTransform<?, ?> transform = node.getTransform();
		currentTransform = AppliedPTransform.of(node.getInput(), node.getOutput(), (PTransform<PInput, POutput>) node.getTransform());

		if (transform != null) {
			TransformTranslator<?> translator = FlinkStreamingTransformTranslators.getTranslator(transform);

			if (translator != null) {
				System.out.println(genSpaces(this.depth) + "doingCompositeTransform- " + formatNodeName(node));
				applyTransform(transform, node, translator);
				inComposite = false;
			}
		}

		this.depth--;
		System.out.println(genSpaces(this.depth) + "leaveCompositeTransform- " + formatNodeName(node));
	}

	@Override
	public void visitTransform(TransformTreeNode node) {
		System.out.println(genSpaces(this.depth) + "visitTransform- " + formatNodeName(node));
		if (inComposite) {
			// ignore it
			return;
		}

		// the transformation applied in this node
		PTransform<?, ?> transform = node.getTransform();
		currentTransform = AppliedPTransform.of(node.getInput(), node.getOutput(), (PTransform<PInput, POutput>) node.getTransform());

		// the translator to the Flink operation(s)
		TransformTranslator<?> translator = FlinkStreamingTransformTranslators.getTranslator(transform);

		if (translator == null) {
			System.out.println(node.getTransform().getClass());
			throw new UnsupportedOperationException("The transform " + transform + " is currently not supported.");
		}

		applyTransform(transform, node, translator);
	}

	@Override
	public void visitValue(PValue value, TransformTreeNode producer) {
		// do nothing here
	}

	/**
	 * Utility method to define a generic variable to cast the translator and the transform to.
	 */
	private <T extends PTransform<?, ?>> void applyTransform(PTransform<?, ?> transform, TransformTreeNode node, TransformTranslator<?> translator) {

		@SuppressWarnings("unchecked")
		T typedTransform = (T) transform;

		@SuppressWarnings("unchecked")
		TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;

		typedTranslator.translateNode(typedTransform, this);
	}

	// --------------------------------------------------------------------------------------------
	//  Translation Context Methods
	// --------------------------------------------------------------------------------------------

	@Override
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return env;
	}

	@Override
	public PipelineOptions getPipelineOptions() {
		return options;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> DataStream<T> getInputDataStream(PInput value) {
		return (DataStream<T>) dataStreams.get(value);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> WindowedDataStream<T> getInputWindowedDataStream(PInput value) {
		return (WindowedDataStream<T>) windowedDataStreams.get(value);
	}

	@Override
	public void setOutputDataStream(POutput value, DataStream<?> stream) {
		if (!dataStreams.containsKey(value)) {
			dataStreams.put(value, stream);
		}
	}

	@Override
	public void setOutputWindowedDataStream(POutput value, WindowedDataStream<?> stream) {
		if (!windowedDataStreams.containsKey(value)) {
			windowedDataStreams.put(value, stream);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> TypeInformation<T> getTypeInfo(POutput output) {
		if (output instanceof TypedPValue) {
			Coder<?> outputCoder = ((TypedPValue) output).getCoder();
			if (outputCoder instanceof KvCoder) {
				return new KvCoderTypeInformation((KvCoder) outputCoder);
			} else {
				return new CoderTypeInformation(outputCoder);
			}
		}
		return new GenericTypeInfo<T>((Class<T>)Object.class);
	}

	@Override
	public PInput getInput(PTransform<?, ?> transform) {
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.transform == transform, "can only be called with current transform");
		return this.currentTransform.input;
	}

	@Override
	public PValue getOutput(PTransform<?, ?> transform) {
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.transform == transform, "can only be called with current transform");
		return (PValue) this.currentTransform.output;
	}

	public CoderRegistry getCoderRegistry(PTransform<?, ?> transform){
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.transform == transform, "can only be called with current transform");
		return pipeline.getCoderRegistry();
	}

	// --------------------------------------------------------------------------------------------
	//  Other functionality
	// --------------------------------------------------------------------------------------------

	/**
	 * A translator of a {@link com.google.cloud.dataflow.sdk.transforms.PTransform}.
	 */
	public static interface TransformTranslator<Type extends PTransform> {

		void translateNode(Type transform, StreamingTranslationContext context);
	}
}
