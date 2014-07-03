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
package eu.stratosphere.tutorial.answerKey.task2;

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
		
		DataSet<Tuple3<Integer, String, Integer>> counts = 
				text.flatMap(new TermFreqMapper());

		// emit result
		if(fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
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

			//Split by "," in order to get the full doc ID (useful in case it is more than one digit)
			Scanner valueTerms = new Scanner(value);

			// Set delimiters to comma.
			valueTerms.useDelimiter(",");
			
			//Saves the docID
			Integer docID = Integer.parseInt(valueTerms.next());
			
			//Saves the rest of the 'document', meaning the content which will then be splitted into words
			value = valueTerms.nextLine().substring(1);
			valueTerms.close();

			//The HashSet will avoid repetition.
			HashMap<String, Integer> hsOccurrence = new HashMap<String, Integer>();
			// First normalize and split the line
			// then emit the pairs
			for (String token : value.toLowerCase().split("\\W+")) {
				if (token.length() > 0) {
					//If a word was not previously seen, we add it
					if (!hsOccurrence.containsKey(token)){
						hsOccurrence.put(token, 1);
					//Now if it does exist we update its frequency - increase by +1
					} else {
						hsOccurrence.put(token, hsOccurrence.get(token)+1);
					}
				}
			}
			
			//checks if the individual terms are not stopwords
			for (String el : hsOccurrence.keySet() ) {
				if (!Util.STOP_WORDS.contains(el)){
					out.collect(new Tuple3<Integer, String, Integer>(docID, el, hsOccurrence.get(el)));
				}
			}
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
