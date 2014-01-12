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

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * This matcher computes the tf-idf weight of every term by combining the results of the previous document and term
 * frequency computation.
 */
public class TfIdfMatcher extends JoinFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Computes the Tf-Idf weight of every term by combining the results of the previous document and term
	 * frequency computations.
	 */
	@Override
	public void join(Record dfRecord, Record tfRecord, Collector<Record> collector) throws Exception {
		// Implement your solution here
	}
}
