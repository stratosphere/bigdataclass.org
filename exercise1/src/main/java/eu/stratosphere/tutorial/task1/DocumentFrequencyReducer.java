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

import java.util.Iterator;

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * This reducer is part of the document frequency computation. See {@link DocumentFrequencyMapper} for an
 * explanation of what the document frequency is.
 * <p>
 * The reduce method will be called grouped by each term.
 */
@Combinable
@ConstantFields(0)
public class DocumentFrequencyReducer extends ReduceFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Adds up all (term, 1) records emitted by {@link DocumentFrequencyMapper} grouped for each term.
	 * <p>
	 * If the inputs are records (big, 1) and (big, 1), the reduce method will be called with an iterator over
	 * both records.
	 */
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> collector) throws Exception {
		if (!records.hasNext()) {
			return;
		}

		Record first = records.next();
		String word = extractFirstStringValue(first);
		int cnt = extractSecondIntValue(first);

		while (records.hasNext()) {
			Record next = records.next();
			String nextWord = extractFirstStringValue(next);
			Validate.isTrue(word.equals(nextWord), "all words returned by the iterator must "
					+ "be the same, but got %s and %s", word, nextWord);
			cnt = cnt + extractSecondIntValue(next);
		}

		collector.collect(new Record(new StringValue(word), new IntValue(cnt)));
	}

	private static String extractFirstStringValue(Record rec) {
		return rec.getField(0, StringValue.class).toString();
	}

	private static int extractSecondIntValue(Record rec) {
		return rec.getField(1, IntValue.class).getValue();
	}

}
