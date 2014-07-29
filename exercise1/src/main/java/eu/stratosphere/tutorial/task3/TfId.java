
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
package eu.stratosphere.tutorial.task3;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.tutorial.task1.DocumentFrequency.*;
import eu.stratosphere.tutorial.task2.TermFrequency.TermFreqMapper;
import eu.stratosphere.tutorial.tempTextData.TempTextData;
import eu.stratosphere.tutorial.util.Util;

/**
 * Task 3:
 * TfId utilizes the mapping and reducing implemented in
 * <p>
 * first DocumentFrquency and second in TermFrequency.
 * It then uses a local UDF, TfIdCount which computes the TfId of each significant word.
 * Finally, it calls a final reduce method that outputs the final result in a new data type, WeightVector.
 */
@SuppressWarnings("serial")
public class TfId {
	
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
		DataSet<String> input = getTextDataSet(env);
		
		
//		-- insert code here --
		
		//DataSet<Tuple2<String, Integer>> inputDocFreqs = 
		//		...
		
		//DataSet<Tuple3<Integer, String, Integer>> inputTermFreqs =
		//		...

		//DataSet<Tuple3<Integer, String, Double>> result =
		//		...		Hint: involves a join
		
		//DataSet<WeightVector> finalOutput =
		//		...
		
//-----------------------------------------------------------------------------------------------------

		
		// emit final output
		if(fileOutput) {
//			finalOutput.writeAsCsv(outputPath, "\n", " ");			//Un-comment once the steps above are implemented
		} else {
//			finalOutput.print();									//Un-comment once the steps above are implemented
		}
		
		// execute program
		env.execute("Task Three - TfId");
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into 
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	
	// Join function that joins a custom POJO with a Tuple
	public static class TfIdCount
	         extends JoinFunction<Tuple2<String, Integer>, Tuple3<Integer, String, Integer>,Tuple3<Integer, String, Double>> {

	  @Override
	  public Tuple3<Integer, String, Double> join(Tuple2<String, Integer> inputDocFreq, Tuple3<Integer, String, Integer> inputTermFreq) {
	  
	    // multiply the points and rating and construct a new output tuple
	    return new Tuple3<Integer, String, Double>(inputTermFreq.f0,inputTermFreq.f1,inputTermFreq.f2 * Math.log(Util.NUM_DOCUMENTS/inputDocFreq.f1));
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
				System.err.println("Usage: TfId <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing TfId example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: TfId <text path> <result path>");
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
