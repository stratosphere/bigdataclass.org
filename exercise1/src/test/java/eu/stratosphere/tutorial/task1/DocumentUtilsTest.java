package eu.stratosphere.tutorial.task1;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author Alexey Grigorev
 */
public class DocumentUtilsTest {

	@Test
	public void splitTheDocumentString() {
		String document = "doc1, Hallo dere!";
		String docId = DocumentUtils.extractDocId(document);
		assertEquals("doc1", docId);

		String contents = DocumentUtils.extractContents(document);
		assertEquals("hallo dere!", contents);
	}

}
