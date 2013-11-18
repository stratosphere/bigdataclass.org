package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import scala.collection.mutable.HashMap

class Task2 extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([numSubtasks])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val numSubTasks = if (args.size >= 3) args(2).toInt else 1
    
    val source = TextFile(inputPath)
    
    // Here you should split the line into the doc id and the content.
    // Then output a (word, count) tuple for every word in the document
    // You could use a map to keep counts of the words.
    val termFrequencies = source flatMap { line =>
    }
    
    val sink = termFrequencies.write(outputPath, CsvOutputFormat("\n", ","))
    
    new ScalaPlan(Seq(sink))
    
  }
}

object RunTask2 {
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
    // big 1
    // hello 1
    // stratosphere 1
    //
    // Document 2:
    // hello 1
    // big 2
    // data 1

    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    val plan = new Task2().getPlan(inputPath, outputPath)
    Util.executePlan(plan)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
}
