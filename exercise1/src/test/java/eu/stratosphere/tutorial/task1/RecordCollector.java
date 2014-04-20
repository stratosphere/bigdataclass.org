package eu.stratosphere.tutorial.task1;

import java.util.List;

import com.google.common.collect.Lists;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * @author Alexey Grigorev
 */
public class RecordCollector implements Collector<Record> {

	private final List<Record> collected = Lists.newLinkedList();

	@Override
	public void collect(Record record) {
		collected.add(record);
	}

	@Override
	public void close() {
	}

	public List<Record> getCollected() {
		return collected;
	}

	public List<String> getCollectedStrings() {
		List<String> res = Lists.newArrayList();

		for (Record record : collected) {
			res.add(extractString(record));
		}

		return res;
	}

	private static String extractString(Record record) {
		return record.getField(0, StringValue.class).toString();
	}

}
