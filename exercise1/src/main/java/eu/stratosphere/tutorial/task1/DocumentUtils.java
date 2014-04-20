package eu.stratosphere.tutorial.task1;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.stratosphere.tutorial.util.Util;

/**
 * @author Alexey Grigorev
 */
public class DocumentUtils {

	public static String extractDocId(String document) {
		int firstComma = document.indexOf(',');
		return document.substring(0, firstComma).trim();
	}

	public static int extractDocIdAsInt(String document) {
		return Integer.parseInt(extractDocId(document));
	}
	
	public static String extractContents(String document) {
		int firstComma = document.indexOf(',');
		return document.substring(firstComma + 1).toLowerCase(Locale.ENGLISH).trim();
	}

	public static List<String> extractTokens(String contents) {
		List<String> result = Lists.newArrayList();

		String[] tokens = contents.split("(\\s+|[,.?!;])");
		for (int i = 0; i < tokens.length; i++) {
			String token = tokens[i];
			if (nonEmptyAlphanumeric(token) && notStopWord(token)) {
				result.add(token);
			}
		}

		return result;
	}

	public static Set<String> extractDistinctTokens(String contents) {
		// TODO: redundancy 
		Set<String> result = Sets.newLinkedHashSet();

		String[] tokens = contents.split("(\\s+|[,.?!;])");
		for (int i = 0; i < tokens.length; i++) {
			String token = tokens[i];
			if (nonEmptyAlphanumeric(token) && notStopWord(token)) {
				result.add(token);
			}
		}

		return result;
	}

	private static boolean notStopWord(String token) {
		return !Util.STOP_WORDS.contains(token);
	}

	private static boolean nonEmptyAlphanumeric(String token) {
		return StringUtils.isNotEmpty(token) && StringUtils.isAlphanumeric(token);
	}

}
