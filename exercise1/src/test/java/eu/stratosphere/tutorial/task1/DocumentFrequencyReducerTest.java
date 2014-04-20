package eu.stratosphere.tutorial.task1;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

/**
 * @author Alexey Grigorev
 */
public class DocumentFrequencyReducerTest {

	@Test
	public void documentFrequencyReduce() throws Exception {
		DocumentFrequencyReducer reducer = new DocumentFrequencyReducer();

		Record r = new Record(new StringValue("word"), new IntValue(1));
		Iterator<Record> records = Arrays.asList(r, r, r, r, r).iterator();

		RecordCollector collector = new RecordCollector();
		reducer.combine(records, collector);

		List<Record> reduceResult = collector.getCollected();
		assertEquals(1, reduceResult.size());
		Record reducedRecord = reduceResult.get(0);

		String word = reducedRecord.getField(0, StringValue.class).getValue();
		assertEquals("word", word);

		int count = reducedRecord.getField(1, IntValue.class).getValue();
		assertEquals(5, count);
	}

}
