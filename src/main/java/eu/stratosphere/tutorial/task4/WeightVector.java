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
package eu.stratosphere.tutorial.task4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.tutorial.util.Util;

/**
 * This is a custom Value implementation for a weight vector, which maps terms (String) to a weight (Double).
 */
public class WeightVector implements Value {

	private static final long serialVersionUID = 1L;

	// - Internal state -----------------------------------------------------------------------------------------------


	// ----------------------------------------------------------------------------------------------------------------

	public WeightVector() {
	}

	/**
	 * Sets the document ID.
	 * 
	 * @param docId
	 *        Document ID
	 */
	public void setDocId(int docId) {
		// Implement your solution here
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
		// Implement your solution here
	}

	/**
	 * Clears the contents of the vector.
	 */
	public void clear() {
		// Implement your solution here
	}

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Serializes the contents of the vector to DataOutput.
	 * <p>
	 * Use DataOutput to serialize the internal state.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// Implement your solution here
	}

	/**
	 * Deserializes the contents of the vector from DataInput.
	 * <p>
	 * Use DataInput to deserialize to the internal state.
	 */
	@Override
	public void read(DataInput in) throws IOException {
		// Implement your solution here
	}

	/**
	 * String representation of this vector.
	 */
	@Override
	public String toString() {
		// Implement your solution here
		
		return "";
	}

	// - Testing ------------------------------------------------------------------------------------------------------

	public static void main(String[] args) throws IOException {
		Random r = new Random();

		// Use stop words as term set
		String[] terms = new String[Util.STOP_WORDS.size()];
		Util.STOP_WORDS.toArray(terms);

		int numTerms = terms.length;
		int numVectors = 5;
		int maxTermsPerVector = 10;

		// 1. Generate random source vectors
		WeightVector[] sourceVectors = new WeightVector[numVectors];

		for (int docId = 0; docId < numVectors; docId++) {
			WeightVector vector = new WeightVector();
			vector.setDocId(docId);

			for (int i = 0; i < r.nextInt(maxTermsPerVector) + 1; i++) {
				vector.add(terms[r.nextInt(numTerms)], r.nextDouble());
			}

			sourceVectors[docId] = vector;
		}

		// 2. Test implementation
		for (WeightVector vector : sourceVectors) {
			// a) Write
			ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
			DataOutputStream dos = new DataOutputStream(os);

			vector.write(dos);

			// Read
			ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
			DataInputStream dis = new DataInputStream(is);

			WeightVector testVector = new WeightVector();

			testVector.read(dis);

			System.out.println(vector);
			System.out.println(testVector);
			System.out.println("----");
		}
	}
}
