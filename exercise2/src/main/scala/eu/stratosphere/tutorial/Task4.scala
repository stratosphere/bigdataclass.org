package eu.stratosphere.tutorial

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import scala.collection.mutable.HashMap

class Task4 extends PlanAssembler with PlanAssemblerDescription with Serializable {
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
    
    // Copy this from task 3
    val  tfIdf = 
      
    // Here you should combine all the (docId, word, tf-idf) tuples for
    // a document into a single WeightVector which has a docId and an
    // Iterator of (term, tf-idf) tuples.
    val tfIdfPerDocument = tfIdf
      .groupBy {  }
      .reduceGroup { values =>
      }
    
    val sink = tfIdfPerDocument.write(outputPath, DelimitedDataSinkFormat(formatOutput _))
    
    new ScalaPlan(Seq(sink))
  }
  
  def formatOutput(t: (String, Iterator[(String, Double)])) = {
    val terms = t._2 map { case (word, tfIdf) => word + ", " + tfIdf }
    t._1 + ": " + terms.mkString("; ")
  }
  
  case class WeightVector(docId: String, terms: Iterator[(String, Int)])
}

object RunTask4 {
  def main(args: Array[String]) {
    // Write test input to temporary directory
    val inputPath = Util.createTempDir("input")

    Util.createTempFile("input/1.txt", "1,Big Hello to Stratosphere! :-)")
    Util.createTempFile("input/2.txt", "2,Hello to Big Big Data.")

    // Output
    // Replace this with your own path, e.g. "file:///path/to/results/"
    val outputPath = "file:///home/aljoscha/tf-idf-out"

    // Results should be: same Tf-Idf values as in task 3 as a WeightVector per Document

    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    val plan = new Task4().getPlan(inputPath, outputPath)
    Util.executePlan(plan)

    Util.deleteAllTempFiles()
    System.exit(0)
  }
}
