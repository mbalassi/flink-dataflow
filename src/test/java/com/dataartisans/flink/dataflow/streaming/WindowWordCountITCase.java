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
package com.dataartisans.flink.dataflow.streaming;

import com.dataartisans.flink.dataflow.examples.streaming.WindowWordCount;
import com.dataartisans.flink.dataflow.util.FlinkStreamingTestPipeline;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.List;


public class WindowWordCountITCase extends StreamingProgramTestBase {

	protected String resultPath;

	public WindowWordCountITCase(){
	}

	static final String[] WORDS_ARRAY = new String[] {
			"hi there", "hi", "hi sue bob",
			"hi sue", "", "bob hi"};

	static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

	static final String[] COUNTS_ARRAY = new String[] {
			"hi: 5", "there: 1", "sue: 2", "bob: 2"};

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(Joiner.on('\n').join(COUNTS_ARRAY), resultPath);
	}

	@Override
	protected void testProgram() throws Exception {

		Pipeline p = FlinkStreamingTestPipeline.create();

		PCollection<String> input = p.apply(Create.of(WORDS))
				.setCoder(StringUtf8Coder.of());

		PCollection<String> output = input
				.apply(ParDo.of(new WindowWordCount.Tokenizer()))
				.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
				.apply(Count.<String>perElement())
				.apply(ParDo.of(new WindowWordCount.FormatCountsFn()));

		output.apply(TextIO.Write.to(resultPath));
		output.apply(TextIO.Write.to("testoutput"));

		p.run();
	}
}

