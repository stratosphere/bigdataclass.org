package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import scala.collection.mutable.HashMap

class Task3 extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numSubTasks = if (args.size >= 3) args(2).toInt else 1
    
    val source = TextFile(inputPath)
    
    // Copy this from task 1
    val termOccurences = source flatMap { line =>
    }
    
    val documentFrequencies = termOccurences
      .groupBy {  }
      .reduce {  }
    
    // Copy this from task 2
    val termFrequencies = source flatMap { line =>
    }
    
    // Here you should join the document frequencies with
    // the word frequencies and compute the Tf-Idf.
    // The output should be a tuple (docId, word, tf-idf)
    // You can get the total number of documents using Util.NUM_DOCUMENTS.
    val  tfIdf = documentFrequencies
      .join(termFrequencies)
      .where { }
      .isEqualTo {  }
      .map { (left, right) =>
      }
    
    val sink = tfIdf.write(outputPath, CsvOutputFormat("\n", ","))
    
    new ScalaPlan(Seq(sink))
    
  }
}

object RunTask3 {
  def main(args: Array[String]) {
    // Write test input to temporary directory
    val inputPath = Util.createTempDir("input")

    Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)")
    Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.")

    // Output
    // Replace this with your own path, e.g. "file:///path/to/results/"
    val outputPath = "file:///home/aljoscha/tf-idf-out"

    // Results should be:
    //
    // Document 1:
    // 1 big 0.0
    // 1 hello 0.0
    // 1 stratosphere 0.69...
    //
    // Document 2:
    // 2 hello 0.0
    // 2 big 0.0
    // 2 data 0.69...

    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    val plan = new Task3().getPlan(inputPath, outputPath)
    Util.executePlan(plan)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
}
