Flink-Dataflow
--------------

Flink-Dataflow is a Google Dataflow Runner for Apache Flink. It enables you to
run Dataflow programs with Flink as an execution engine.

# Getting Started

To get started using Google Dataflow on top of Apache Flink, we need to install the
latest version of Flink-Dataflow.

## Install Flink-Dataflow ##

To retrieve the latest version of Flink-Dataflow, run the following command

    git clone https://github.com/dataArtisans/flink-dataflow

Then switch to the newly created directory and run Maven to build the Dataflow runner:

    cd flink-dataflow
    mvn clean install -DskipTests

Flink-Dataflow is now installed in your local maven repository.

## Executing an example

Next, let's run the classic WordCount example. It's semantically identically to
the example provided with Google Dataflow. Only this time, we chose the
`FlinkPipelineRunner` to execute the WordCount on top of Flink.

Here's an excerpt from the WordCount class file:

```java
Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
// yes, we want to run WordCount with Flink
options.setRunner(FlinkPipelineRunner.class);

Pipeline p = Pipeline.create(options);

p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
		.apply(new CountWords())
		.apply(TextIO.Write.named("WriteCounts")
				.to(options.getOutput())
				.withNumShards(options.getNumShards()));

p.run();
```


To execute the example, let's first get some sample data:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > kinglear.txt

Then let's run the included WordCount locally on your machine:

    mvn exec:exec -Dinput=kinglear.txt -Doutput=wordcounts.txt

Congratulations, you have run your first Google Dataflow program on top of Apache Flink!


# Running Dataflow on Flink on a cluster

You can run your Dataflow program on a Apache Flink cluster as well. For more
information, please visit the [Apache Flink Website](http://flink.apache.org) or
contact the
[Mailinglists](http://flink.apache.org/community.html#mailing-lists).
