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

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * This matcher computes the tf-idf weight of every term by combining the results of the previous document and term
 * frequency computation.
 */
public class TfIdfMatcher extends MatchStub {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Computes the Tf-Idf weight of every term by combining the results of the previous document and term
	 * frequency computations.
	 */
	@Override
	public void match(PactRecord dfRecord, PactRecord tfRecord, Collector<PactRecord> collector) throws Exception {
		// Implement your solution here
	}
}
