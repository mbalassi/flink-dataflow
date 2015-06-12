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
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TypedPValue;
import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * FlinkPipelineTranslator knows how to translate Pipeline objects into Flink Jobs.
 *
 * This is based on {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator}
 */
public class FlinkPipelineTranslator implements PipelineVisitor, TranslationContext {

	private final static String DEFAULT_TRANSFORM_NAME = "Unknown Transform";

	private final Map<POutput, DataSet<?>> dataSets;
	private final Map<PCollectionView<?>, DataSet<?>> broadcastDataSets;

	private final ExecutionEnvironment env;
	private final PipelineOptions options;
	private Pipeline pipeline;

	private int depth = 0;

	private boolean inComposite = false;

	private AppliedPTransform<?, ?, ?> currentTransform;

	public FlinkPipelineTranslator(ExecutionEnvironment env, PipelineOptions options) {
		this.env = env;
		this.options = options;
		this.dataSets = new HashMap<>();
		this.broadcastDataSets = new HashMap<>();
	}


	public void translate(Pipeline pipeline) {
		this.pipeline = pipeline; // TODO: consider this
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
		PTransform transform = node.getTransform();
		String name = transform == null ? DEFAULT_TRANSFORM_NAME : transform.getName();
		currentTransform = AppliedPTransform.of(name, node.getInput(), node.getOutput(),
				(PTransform<PInput, POutput>) node.getTransform());

		if (transform != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);

//			if (translator != null) {
//				inComposite = true;
//
//				// TODO: CoGroupByKey no longer has this method
//				if (transform instanceof CoGroupByKey &&
//						((CoGroupByKey<?>) node.getInput()).getKeyedCollections().size() != 2) {
//					// we can only optimize CoGroupByKey for input size 2
//					inComposite = false;
//				}
//			}
		}

		this.depth++;
	}

	@Override
	public void leaveCompositeTransform(TransformTreeNode node) {
		PTransform<?, ?> transform = node.getTransform();
		String name = transform == null ? DEFAULT_TRANSFORM_NAME : transform.getName();
		currentTransform = AppliedPTransform.of(name, node.getInput(), node.getOutput(),
				(PTransform<PInput, POutput>) node.getTransform());

		if (transform != null) {
			TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);

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
		String name = transform == null ? DEFAULT_TRANSFORM_NAME : transform.getName();
		currentTransform = AppliedPTransform.of(name, node.getInput(), node.getOutput(),
				(PTransform<PInput, POutput>) node.getTransform());

		// the translator to the Flink operation(s)
		TransformTranslator<?> translator = FlinkTransformTranslators.getTranslator(transform);

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
	public ExecutionEnvironment getExecutionEnvironment() {
		return env;
	}

	@Override
	public PipelineOptions getPipelineOptions() {
		return options;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> DataSet<T> getInputDataSet(PInput value) {
		return (DataSet<T>) dataSets.get(value);
	}

	@Override
	public void setOutputDataSet(POutput value, DataSet<?> set) {
		if (!dataSets.containsKey(value)) {
			dataSets.put(value, set);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> DataSet<T> getSideInputDataSet(PCollectionView<?> value) {
		return (DataSet<T>) broadcastDataSets.get(value);
	}

	@Override
	public void setSideInputDataSet(PCollectionView<?> value, DataSet<?> set) {
		if (!broadcastDataSets.containsKey(value)) {
			broadcastDataSets.put(value, set);
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
	public <InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform) {
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.getTransform() == transform, "can only be called with current transform");
		return (InputT) this.currentTransform.getInput();
	}

	@Override
	public <OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform) {
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.getTransform() == transform, "can only be called with current transform");
		return (OutputT) this.currentTransform.getOutput();
	}

	public CoderRegistry getCoderRegistry(PTransform<?, ?> transform){
		Preconditions.checkArgument(this.currentTransform != null && this.currentTransform.getTransform() == transform, "can only be called with current transform");
		return pipeline.getCoderRegistry();
	}

	// --------------------------------------------------------------------------------------------
	//  Other functionality
	// --------------------------------------------------------------------------------------------

	/**
	 * A translator of a {@link com.google.cloud.dataflow.sdk.transforms.PTransform}.
	 */
	public static interface TransformTranslator<Type extends PTransform> {

		void translateNode(Type transform, TranslationContext context);
	}

}
