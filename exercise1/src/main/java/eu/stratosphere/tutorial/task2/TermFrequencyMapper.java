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
package eu.stratosphere.tutorial.task2;

import java.util.List;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.tutorial.task1.DocumentUtils;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * This mapper computes the term frequency for each term in a document.
 * <p>
 * The term frequency of a term in a document is the number of times the term occurs in the
 * respective document. If a document contains a term three times, the term has a term frequency of
 * 3 (for this document).
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The term frequency of "Big" in document 1 is 3 and 1 in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
public class TermFrequencyMapper extends MapFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Splits the document into terms and emits a PactRecord (docId, term, tf) for each term of the
	 * document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 */
	@Override
	public void map(Record record, Collector<Record> collector) {
		String document = record.getField(0, StringValue.class).toString();

		int docId = DocumentUtils.extractDocIdAsInt(document);
		String contents = DocumentUtils.extractContents(document);

		List<String> tokens = DocumentUtils.extractTokens(contents);
		Multiset<String> tfCounter = HashMultiset.create(tokens);

		for (Entry<String> tfEntry : tfCounter.entrySet()) {
			Record outputRecord = new Record();
			String word = tfEntry.getElement();
			int tf = tfEntry.getCount();

			outputRecord.addField(new IntValue(docId));
			outputRecord.addField(new StringValue(word));
			outputRecord.addField(new IntValue(tf));

			collector.collect(outputRecord);
		}
	}

}