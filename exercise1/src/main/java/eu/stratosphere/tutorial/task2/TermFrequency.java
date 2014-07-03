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
package eu.stratosphere.tutorial.task2;

import java.util.HashMap;
import java.util.Scanner;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.tutorial.tempTextData.TempTextData;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.util.Collector;

/**
 * Task 2:
 * This mapper computes the term frequency for each term in a document.
 * <p>
 * The term frequency of a term in a document is the number of times the term occurs in the respective document. If a
 * document contains a term three times, the term has a term frequency of 3 (for this document).
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The term frequency of "Big" is 3 in document 1 and it is 1 in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
@SuppressWarnings("serial")
public class TermFrequency {
	
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
		
		
//		-- insert code here --
//			Create a new DataSet (with the correct type argument. Hint: Think of tuples)
//			Call it counts. 
//			Apply mapping.
//	-----------------------------------------------------------------------------------------------------


		// emit result
		if(fileOutput) {
//			counts.writeAsCsv(outputPath, "\n", " ");			//Un-comment once the step above is implemented
		} else {
//			counts.print();										//Un-comment once the step above is implemented
		}
		
		// execute program
		env.execute("Task Two - Term frequency");
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * This mapper splits input into words as a user-defined FlatMapFunction. 
	 * The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(2,word,1)" meaning (documentID , single word , count) (Tuple3<Integer, String, Integer>).
	 */
	public static final class TermFreqMapper extends FlatMapFunction<String, Tuple3<Integer, String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple3<Integer, String, Integer>> out) {
			
			
//			-- insert code here --
//			Split the input into distinct words. 
//			Pay attention to the docID. (In this task you will also need to keep track of it.
//			As previously stated, only non STOP_WORDs should be accepted.
//			
//			Hint: 
//			Think of the appropriate data structure that supports 
//			the uniqueness of keys while allowing value update. 
//		-----------------------------------------------------------------------------------------------------

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
				System.err.println("Usage: TermFrequency <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing TermFrequency example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: TermFrequency <text path> <result path>");
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
