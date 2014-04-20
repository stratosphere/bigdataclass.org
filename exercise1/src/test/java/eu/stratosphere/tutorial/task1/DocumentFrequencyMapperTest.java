package eu.stratosphere.tutorial.task1;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

/**
 * @author Alexey Grigorev
 */
public class DocumentFrequencyMapperTest {

	// sut
	DocumentFrequencyMapper mapper = new DocumentFrequencyMapper();

	@Test
	public void simpleWords() {
		Record record = stringRecord("doc1, cool stuff");
		RecordCollector collector = new RecordCollector();

		mapper.map(record, collector);

		List<String> res = collector.getCollectedStrings();
		assertEquals(Arrays.asList("cool", "stuff"), res);
	}

	@Test
	public void repeats() {
		Record record = stringRecord("doc2,F%%%ing cool stuff. Cool stuff.");
		RecordCollector collector = new RecordCollector();

		mapper.map(record, collector);

		List<String> res = collector.getCollectedStrings();

		assertEquals(2, res.size());
		assertEquals(ImmutableSet.of("cool", "stuff"), Sets.newHashSet(res));
	}

	@Test
	public void noStopWords() {
		Record record = stringRecord("doc3,This is awesome.");
		RecordCollector collector = new RecordCollector();

		mapper.map(record, collector);

		List<String> res = collector.getCollectedStrings();
		assertEquals(ImmutableSet.of("awesome"), Sets.newHashSet(res));
	}

	private static Record stringRecord(String value) {
		return new Record(new StringValue(value));
	}

}