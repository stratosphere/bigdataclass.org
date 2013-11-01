package eu.stratosphere.tutorial

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.plan.Plan
import com.google.common.io.Files
import java.io.File
import com.google.common.base.Charsets
import java.io.IOException
import org.apache.commons.io.FileUtils

object Util {
  val tempFiles = new scala.collection.mutable.Queue[File]

  val NUM_DOCUMENTS = 2

  val STOP_WORDS = Set(
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "cannot",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "me",
    "more",
    "most",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "ought",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "same",
    "she",
    "should",
    "so",
    "some",
    "such",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "with",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves")

  def executePlan(plan: Plan) {
    val ex = new LocalExecutor()
    ex.start()
    val runtime = ex.executePlan(plan)
    println("runtime:  " + runtime)
    ex.stop();
  }

  def createTempFile(fileName: String, contents: String) = {
    val f = createAndRegisterTempFile(fileName)
    Files.write(contents, f, Charsets.UTF_8)
    "file://" + f.getAbsolutePath();
  }

  def createTempDir(dirName: String) = {
    val f = createAndRegisterTempFile(dirName)
    "file://" + f.getAbsolutePath()
  }

  def getTempPath() = {
    "file://" + new File(System.getProperty("java.io.tmpdir")).getAbsolutePath()
  }

  def deleteAllTempFiles() {
    tempFiles filter { _.exists } foreach { f => deleteRecursively(f) }
  }

  def createAndRegisterTempFile(fileName: String) = {
    val baseDir = new File(System.getProperty("java.io.tmpdir"))
    val f = new File(baseDir, fileName)

    if (f.exists()) {
      deleteRecursively(f)
    }

    var parentToDelete = f
    var running = true
    while (running) {
      val parent = parentToDelete.getParentFile();
      if (parent == null) {
        throw new IOException("Missed temp dir while traversing parents of a temp file.");
      }
      if (parent.equals(baseDir)) {
        running = false
      } else {
        parentToDelete = parent
      }
    }

    Files.createParentDirs(f)
    tempFiles += parentToDelete
    f
  }

  def deleteRecursively(f: File) {
    if (f.isDirectory()) {
      FileUtils.deleteDirectory(f);
    } else {
      f.delete();
    }
  }

}