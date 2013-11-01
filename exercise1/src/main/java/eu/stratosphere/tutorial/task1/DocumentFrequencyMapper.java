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
package eu.stratosphere.tutorial.task1;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * This mapper is part of the document frequency computation.
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
 * <p>
 * The map method will be called independently for each document.
 */
public class DocumentFrequencyMapper extends MapStub {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Splits the document into terms and emits a PactRecord (term, 1) for each term of the document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 * <p>
	 * Example:
	 * 
	 * <pre>
	 * 1,Gartner's definition (the 3Vs) is still widely used
	 * </pre>
	 * 
	 * The document ID of the document is 1 (start of line before the comma). The terms to be extracted are:
	 * <ul>
	 * <li>gartner</li>
	 * <li>s</li>
	 * <li>definition</li>
	 * <li>3vs</li>
	 * <li>still</li>
	 * <li>widely</li>
	 * </ul>
	 * Note that the stop words "the" and "is" have been removed and everything has been lower cased.
	 */
	@Override
	public void map(PactRecord record, Collector<PactRecord> collector) {
		// Document with format "docId, document contents"
		String document = record.getField(0, PactString.class).toString();

		// Implement your solution here
	}
}