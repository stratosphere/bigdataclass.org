package eu.stratosphere.tutorial.task2;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

import eu.stratosphere.tutorial.task1.RecordCollector;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

/**
 * @author Alexey Grigorev
 */
public class TermFrequencyMapperTest {

	@Test
	public void termFrequency() {
		TermFrequencyMapper termFrequencyMapper = new TermFrequencyMapper();

		RecordCollector collector = new RecordCollector();

		String doc1 = "1,Once upon a time there was a girl who hated Big Data."
				+ " The girl was big and she liked Small Data";
		termFrequencyMapper.map(new Record(new StringValue(doc1)), collector);

		String doc2 = "2,The girl did not like Linux";
		termFrequencyMapper.map(new Record(new StringValue(doc2)), collector);

		Set<String> csv = toCsv(collector.getCollected());
		System.out.println(csv);

		Set<String> expected = Sets.newHashSet();
		expected.add("1,upon,1");
		expected.add("1,time,1");
		expected.add("1,girl,2");
		expected.add("1,hated,1");
		expected.add("1,big,2");
		expected.add("1,data,2");
		expected.add("1,liked,1");
		expected.add("1,small,1");
		expected.add("2,girl,1");
		expected.add("2,like,1");
		expected.add("2,linux,1");

		assertEquals(expected, csv);
	}

	private static Set<String> toCsv(List<Record> records) {
		Set<String> result = Sets.newHashSet();

		for (Record record : records) {
			String csv = record.getField(0, Value.class).toString() + ","
					+ record.getField(1, Value.class).toString() + ","
					+ record.getField(2, Value.class).toString();
			result.add(csv);
		}

		return result;
	}

}
