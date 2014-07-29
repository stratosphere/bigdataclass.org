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
package eu.stratosphere.tutorial.util;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.nephele.client.JobExecutionResult;


public class Util {

	private static final File TEMP_DIR = new File(System.getProperty("java.io.tmpdir"));

	private static final List<File> tempFiles = new ArrayList<File>();

	public static double NUM_DOCUMENTS = 2;

	public static final HashSet<String> STOP_WORDS = new HashSet<String>();

	static {
		STOP_WORDS.add("a");
		STOP_WORDS.add("about");
		STOP_WORDS.add("above");
		STOP_WORDS.add("after");
		STOP_WORDS.add("again");
		STOP_WORDS.add("against");
		STOP_WORDS.add("all");
		STOP_WORDS.add("am");
		STOP_WORDS.add("an");
		STOP_WORDS.add("and");
		STOP_WORDS.add("any");
		STOP_WORDS.add("are");
		STOP_WORDS.add("as");
		STOP_WORDS.add("at");
		STOP_WORDS.add("be");
		STOP_WORDS.add("because");
		STOP_WORDS.add("been");
		STOP_WORDS.add("before");
		STOP_WORDS.add("being");
		STOP_WORDS.add("below");
		STOP_WORDS.add("between");
		STOP_WORDS.add("both");
		STOP_WORDS.add("but");
		STOP_WORDS.add("by");
		STOP_WORDS.add("cannot");
		STOP_WORDS.add("could");
		STOP_WORDS.add("did");
		STOP_WORDS.add("do");
		STOP_WORDS.add("does");
		STOP_WORDS.add("doing");
		STOP_WORDS.add("down");
		STOP_WORDS.add("during");
		STOP_WORDS.add("each");
		STOP_WORDS.add("few");
		STOP_WORDS.add("for");
		STOP_WORDS.add("from");
		STOP_WORDS.add("further");
		STOP_WORDS.add("had");
		STOP_WORDS.add("has");
		STOP_WORDS.add("have");
		STOP_WORDS.add("having");
		STOP_WORDS.add("he");
		STOP_WORDS.add("her");
		STOP_WORDS.add("here");
		STOP_WORDS.add("hers");
		STOP_WORDS.add("herself");
		STOP_WORDS.add("him");
		STOP_WORDS.add("himself");
		STOP_WORDS.add("his");
		STOP_WORDS.add("how");
		STOP_WORDS.add("i");
		STOP_WORDS.add("if");
		STOP_WORDS.add("in");
		STOP_WORDS.add("into");
		STOP_WORDS.add("is");
		STOP_WORDS.add("it");
		STOP_WORDS.add("its");
		STOP_WORDS.add("itself");
		STOP_WORDS.add("me");
		STOP_WORDS.add("more");
		STOP_WORDS.add("most");
		STOP_WORDS.add("my");
		STOP_WORDS.add("myself");
		STOP_WORDS.add("no");
		STOP_WORDS.add("nor");
		STOP_WORDS.add("not");
		STOP_WORDS.add("of");
		STOP_WORDS.add("off");
		STOP_WORDS.add("on");
		STOP_WORDS.add("once");
		STOP_WORDS.add("only");
		STOP_WORDS.add("or");
		STOP_WORDS.add("other");
		STOP_WORDS.add("ought");
		STOP_WORDS.add("our");
		STOP_WORDS.add("ours");
		STOP_WORDS.add("ourselves");
		STOP_WORDS.add("out");
		STOP_WORDS.add("over");
		STOP_WORDS.add("own");
		STOP_WORDS.add("same");
		STOP_WORDS.add("she");
		STOP_WORDS.add("should");
		STOP_WORDS.add("so");
		STOP_WORDS.add("some");
		STOP_WORDS.add("such");
		STOP_WORDS.add("than");
		STOP_WORDS.add("that");
		STOP_WORDS.add("the");
		STOP_WORDS.add("their");
		STOP_WORDS.add("theirs");
		STOP_WORDS.add("them");
		STOP_WORDS.add("themselves");
		STOP_WORDS.add("then");
		STOP_WORDS.add("there");
		STOP_WORDS.add("these");
		STOP_WORDS.add("they");
		STOP_WORDS.add("this");
		STOP_WORDS.add("those");
		STOP_WORDS.add("through");
		STOP_WORDS.add("to");
		STOP_WORDS.add("too");
		STOP_WORDS.add("under");
		STOP_WORDS.add("until");
		STOP_WORDS.add("up");
		STOP_WORDS.add("very");
		STOP_WORDS.add("was");
		STOP_WORDS.add("we");
		STOP_WORDS.add("were");
		STOP_WORDS.add("what");
		STOP_WORDS.add("when");
		STOP_WORDS.add("where");
		STOP_WORDS.add("which");
		STOP_WORDS.add("while");
		STOP_WORDS.add("who");
		STOP_WORDS.add("whom");
		STOP_WORDS.add("why");
		STOP_WORDS.add("with");
		STOP_WORDS.add("would");
		STOP_WORDS.add("you");
		STOP_WORDS.add("your");
		STOP_WORDS.add("yours");
		STOP_WORDS.add("yourself");
		STOP_WORDS.add("yourselves");
	}

	public static void executePlan(Plan toExecute) throws Exception {
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		JobExecutionResult res = executor.executePlan(toExecute);
		print("runtime:  " + res.getNetRuntime());
		executor.stop();
	}

	public static String createTempFile(String fileName, String contents) throws IOException {
		File f = createAndRegisterTempFile(fileName);
		Files.write(contents, f, Charsets.UTF_8);
		return absolutePath(f);
	}

	private static String absolutePath(File f) {
		if (SystemUtils.IS_OS_WINDOWS) {
			String urlEncodedPath = f.toURI().toString();
			return decodePath(urlEncodedPath);
		} else {
			return "file://" + f.getAbsolutePath();
		}
	}

	private static String decodePath(String urlEncodedPath) {
		try {
			return URLDecoder.decode(urlEncodedPath, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw Throwables.propagate(e);
		}
	}

	public static String createTempDir(String dirName) {
		File tempDir = new File(TEMP_DIR, dirName);
		tempDir.mkdirs();
		tempFiles.add(tempDir);
		return absolutePath(tempDir);
	}

	public static String getTempPath() {
		return absolutePath(TEMP_DIR);
	}

	public static void deleteAllTempFiles() throws IOException {
		for (File f : tempFiles) {
			if (f.exists()) {
				deleteRecursively(f);
			}
		}
	}

	private static File createAndRegisterTempFile(String fileName) throws IOException {
		File baseDir = TEMP_DIR;
		File f = new File(baseDir, fileName);

		if (f.exists()) {
			deleteRecursively(f);
		}

		File parentToDelete = f;
		while (true) {
			File parent = parentToDelete.getParentFile();
			if (parent == null) {
				throw new IOException("Missed temp dir while traversing parents of a temp file.");
			}
			if (parent.equals(baseDir)) {
				break;
			}
			parentToDelete = parent;
		}

		Files.createParentDirs(f);
		tempFiles.add(parentToDelete);
		return f;
	} 

	private static void deleteRecursively(File f) throws IOException {
		if (f.isDirectory()) {
			FileUtils.deleteDirectory(f);
		} else {
			f.delete();
		}
	}

	public static void showResults(String outputPath) throws Exception {
		print("Results");

		URI uri = new URI(outputPath);
		File output = new File(uri.getPath());
		File[] listFiles = output.listFiles();
		if (listFiles == null) {
			print("Nothing");
			return;
		}

		for (File f : listFiles) {
			print("file: " + f);
			List<String> lines = FileUtils.readLines(f);
			for (String line : lines) {
				print("--> " + line);
			}
		}
	}

	private static void print(String s) {
		System.out.println(s);
	}

}