/***********************************************************************************************************************
*
* Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.tutorial.answerKey.task1;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.tutorial.tempTextData.TempTextData;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.util.Collector;
import eu.stratosphere.tutorial.util.*;


/**
 * Task 1:
 * <p>
 * The document frequency of a term is the number of documents it occurs in. If a document contains a term three times,
 * it is counted only once for this document. But if another document contains the word as well, the overall frequency
 * is two.
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The document frequency of "Big" is 2, because it appears in two documents (even though it appears four times in
 * total). "Hello" has a document frequency of 1, because it only appears in document 2.
 */

@SuppressWarnings("serial")
public class DocumentFrequency {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	
	public static void main(String[] args) throws Exception {
		
		if(!parseParameters(args)) {
			return;
		}
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// get input data
		DataSet<String> text = getTextDataSet(env);
		
		DataSet<Tuple2<String, Integer>> counts = 
				text.flatMap(new DocumentFrequencyMapper())
				// group by the tuple field "0"
				.groupBy(0)
				.reduceGroup(new DocumentFrequencyReducer());
				
		// emit result
		if(fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}
		
		// execute program
		env.execute("Task One Document frequency");
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	

	/**
	 * DocumentFrequencyMapper extends the FlatMapFunction.
	 * Processes the input by splitting each line so that only alphanumerical words are accepted.
	 * The map method will be called independently for each document.
	 */
	public static final class DocumentFrequencyMapper extends FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

			//Split by "," in order to get the full doc ID (useful in case it is more than one digit)
			Scanner valueTerms = new Scanner(value);

			// Set delimiters to comma.
			valueTerms.useDelimiter(",");
			
			//Skips the docID
			valueTerms.next();
			//Saves the rest of the 'document', meaning the content which will then be splitted into words
			value = valueTerms.nextLine().substring(1);
			valueTerms.close();

			HashSet hsOccurrence = new HashSet();
			// First normalize and split the line
			// then emit the pairs
			for (String token : value.toLowerCase().split("\\W+")) {
				if (token.length() > 0) {
					//The HashSet will avoid repetition.
					if (hsOccurrence.contains(token)) continue;

					//Word appears in the first time in the sentence
					hsOccurrence.add(token);

					if (!Util.STOP_WORDS.contains(token)){
						out.collect(new Tuple2<String, Integer>(token, 1));
					}	
				}
			}
		}
	}
	
	/**
	 * DocumentFrequencyReducer extends the GroupReduceFunction.
	 * Adds up all (term, 1) records emitted by the mapper grouped for each term.
	 * <p>
	 * If the inputs are records (big, 1) and (big, 1), the reduce method will be called with an iterator over both
	 * records.
	 */
	public static final class DocumentFrequencyReducer extends GroupReduceFunction<Tuple2<String, Integer>,Tuple2<String, Integer>> {

		@Override
		public void reduce(Iterator<Tuple2<String, Integer>> values,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			
			Tuple2<String, Integer> tup = values.next();
			String key = tup.f0;
			int intSum = tup.f1;
			while(values.hasNext()){
				tup = values.next();
				intSum += tup.f1;
			}
			out.collect(new Tuple2<String, Integer>(key, intSum));
			
		}
	}

	
	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	
	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;
	
	private static boolean parseParameters(String[] args) {
		
		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: DocumentFrequency <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing DocumentFrequency example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: DocumentFrequency <text path> <result path>");
		}
		return true;
	}
	
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return TempTextData.getDefaultTextLineDataSet(env);
		}
	}
	

}
