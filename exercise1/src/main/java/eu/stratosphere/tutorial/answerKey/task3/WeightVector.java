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
package eu.stratosphere.tutorial.answerKey.task3;

import java.util.ArrayList;

/**
 * This is a custom Value implementation for a weight vector, which maps terms (String) to a weight (Double).
 */
public class WeightVector {

	private static final long serialVersionUID = 1L;
	private int docId;

	
	private ArrayList<String> termList = new ArrayList<String>();
	private ArrayList<Double> weightList = new ArrayList<Double>();

	//Constructor - no arguments
	public WeightVector() {
	}

	/**
	 * Sets the document ID.
	 * 
	 * @param docId
	 *        Document ID
	 */
	public void setDocId(int docId) {
		this.docId = docId;
	}

	/**
	 * Adds a term with a given weight to the vector.
	 * 
	 * @param term
	 *        Term to add
	 * @param weight
	 *        Weight of term
	 */
	public void add(String term, double weight) {
		termList.add(term);
		weightList.add(weight);
	}

	/**
	 * Clears the contents of the vector.
	 */
	public void clear() {
		termList.clear();
		weightList.clear();
	}
	
	/**
	 * String representation of this vector.
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (int i=0; i<termList.size();i++){
			builder.append(" , ("+termList.get(i)+" , ");
			builder.append(weightList.get(i)+")");
		}
		return "("+docId+builder.toString()+")";
	}
}
