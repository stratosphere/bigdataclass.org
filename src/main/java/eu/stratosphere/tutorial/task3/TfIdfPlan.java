/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.tutorial.task3;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.tutorial.task1.DocumentFrequencyMapper;
import eu.stratosphere.tutorial.task1.DocumentFrequencyReducer;
import eu.stratosphere.tutorial.task2.TermFrequencyMapper;
import eu.stratosphere.tutorial.util.Util;

/**
 * Task 3: Plan for Tf-Idf weight computation.
 */
public class TfIdfPlan implements PlanAssembler, PlanAssemblerDescription {

	@Override
	public String getDescription() {
		return "Usage: [inputPath] [outputPath] ([numSubtasks])";
	}

	@Override
	public Plan getPlan(String... args) {

		String inputPath = args.length >= 1 ? args[0] : "";
		String outputPath = args.length >= 2 ? args[1] : "";
		int numSubtasks = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "Input Documents");

		// - Task 1: Document Frequency -------------------------------------------------------------------------------

		MapContract dfMapper = MapContract.builder(DocumentFrequencyMapper.class)
			.input(source)
			.name("Document Frequency Mapper")
			.build();

		ReduceContract dfReducer = ReduceContract.builder(DocumentFrequencyReducer.class, PactString.class, 0)
			.input(dfMapper)
			.name("Document Frequency Reducer")
			.build();

		// - Task 2: Term Frequency -----------------------------------------------------------------------------------

		MapContract tfMapper = MapContract.builder(TermFrequencyMapper.class)
			.input(source)
			.name("Term Frequency Mapper")
			.build();

		// - Task 3: Term and Document Frequency Match ----------------------------------------------------------------

		MatchContract dfTfMatcher = MatchContract.builder(TfIdfMatcher.class, PactString.class, 0, 1)
			.input1(dfReducer)
			.input2(tfMapper)
			.name("Tf-Idf Matcher")
			.build();

		FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, dfTfMatcher, "Tf-Idf Weights");
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactInteger.class, 0) // document ID
			.field(PactString.class, 1) // term
			.field(PactDouble.class, 2); // tf-idf

		Plan plan = new Plan(sink, "Tf-Idf Computation");
		plan.setDefaultParallelism(numSubtasks);

		return plan;
	}

	public static void main(String[] args) throws Exception {
		// Write test input to temporary directory
		String inputPath = Util.createTempDir("input");

		Util.NUM_DOCUMENTS = 2;

		Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)");
		Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.");

		// Output
		// Replace this with your own path, e.g. "file:///path/to/results/"
		String outputPath = Util.createTempDir("results");

		// Results should be:
		//
		// Document 1:
		// 1 big 0.0
		// 1 hello 0.0
		// 1 stratosphere 0.69...
		//
		// Document 2:
		// 2 hello 0.0
		// 2 big 0.0
		// 2 data 0.69...

		System.out.println("Reading input from " + inputPath);
		System.out.println("Writing output to " + outputPath);

		Plan toExecute = new TfIdfPlan().getPlan(inputPath, outputPath);
		Util.executePlan(toExecute);

		Util.deleteAllTempFiles();
	}
}