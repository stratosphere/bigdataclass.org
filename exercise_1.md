---
layout:	default_exercise
title:  "Exercise 1: Tf-Idf (Java)"
tasks: 
  -       { anchor: overview, title: "Overview"}
  -       { anchor: task1, title: "1: Document Frequency" }
  -       { anchor: task2", title: "2: Term Frequency" }
  -       { anchor: task3, title: "3: Join" }
  -       { anchor: task4, title: "4: WeightVector per Document" }
---

<section id="overview">
## Overview

In this programming assignment, you are going to compute [tf-idf](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) weights for a small dataset in Stratosphere. You will be introduced to the main concepts in the respective subtasks.

The following figure gives an overview of what we are going to do in each task:

<img src="plan.svg" onerror="this.onerror=null; this.src='plan.svg'" class="img-rounded">


<button class="btn btn-primary" id="show-plan-example">Show more detailed example</button>

<img id="plan-example" style="display:none;" src="plan-example.svg" onerror="this.onerror=null; this.src='plan-example.svg'" class="img-rounded">

</section>

<section id="task1">
## Task 1: Document Frequency
<div class="progress">
  <div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 1%;">
    <span class="sr-only">1% Complete</span>
  </div>
</div>

You are first going to calculate the document frequency. That is, in how many documents each distinct word occurs.
So if a document contains a word three times, it is counted only once for this document. But if another document contains the word as well, the overall frequency is two.

Besides this, you need to do some data cleansing on the input data. That is, accept only terms (words) that are alphanumerical. There is also a list of stopwords (provided by us) which should be rejected.

To achieve this, you need to implement a Mapper and a Reducer. The input file is a regular text file that contains a line for each document.
The schema of the input file looks like this:
```
docid, document contents
```
The input text data will first apply a flatMap and later a reduce method. These are both overrides under respectively a mapper class that extends the abstract class FlatMapFunction and a reducer class that extends the abstract class GroupReduceFunction.
Remember to "group by" the correct index. (In our case the first field: the string representing the splitted words)

Further instructions: 

-The mapper class: DocumentFrequencyMapper
You will need to split the input line so that only alphanumerical words are accepted. Remember to remove the document id. (Hint: In case that the length of the docId is unknown, you can opt to use a scanner that uses "," as its delimiter.)
Next, you will need to check if the individual terms are in the HashSet (`Util.STOP_WORDS`) of stopwords. You do not need to do additional extra checks because we already checked the input.

You should also use a [HashSet](http://docs.oracle.com/javase/7/docs/api/java/util/HashSet.html) to check if the word already occured for the given document. Remember, repeted words in the same docuement are only counted once.

The last step is to emit all words with a frequency count of 1. (Hint: the tuple's second field type is Integer.) The output collectors can be used as follows: 
```
<nameOfCollector>.collect(new Tuple2<String, Integer>(... , ...))
```
-The reducer class: DocumentFrequencyReducer
The reducer will take over the mapper. It uses an iterator to go through the previously collected tuple. While doing so, it adds up the number of times a word has occured.
The result is then sent to the collector.
```
out.collect(new Tuple2<String, Integer>(key, intSum));
```

Use the provided `main()` method to test your code.

Task #1 is quite similar to the classical WordCount example, which in Big Data is analogous to "Hello World".
</section>

<a name="task2"></a>
<h2 class="page-header">Task 2: Term Frequency</h2>
<div class="progress">
  <div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 20%;">
    <span class="sr-only">20% Complete</span>
  </div>
</div>

Implement a second Mapper that also reads in the documents from the same file. 
This time, the output tuples shall look like this `(docid, word, frequency)`.

You should use the same splitter for the input line as in Task #1. But this time, use the `docid`. 
Use a HashMap to identify the frequency of each word in the document.
The result will as usual be outputted to the collector:
```
out.collect(new Tuple3<Integer, String, Integer>(... , ... , ...);
```

<section id="task3">
## Task 3: Join

<div class="progress">
  <div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 40%;">
    <span class="sr-only">40% Complete</span>
  </div>
</div>

### Preparation - WeightVector

Stratosphere comes with a variety of build-in datatypes. For this assignment, you are going to implement your own. This step is a preparation for the last one. We want to store a `WeightVector`.
The datatype has an `add(String, double)` method to store the weight of each word.

### TfId

This class can be viewed as the plan. It brings together four main classes:
- the previously implemented DocumentFrequency and TermFrequency
- the newly created TfIdCount which is a class extending JoinFunction (see USER FUNCTIONS section)
- the provided ReduceToVector class.

Further instructions:

TfIdCount outputs a new tuple with the following field types
```
Tuple3<Integer, String, Double>
```
You will need to override the join method.

The following pseudo code describes what the join does.

```javascript
join( (word, df), (docid, word, tf)) {
	tf_idf(word) = tf * log [Util.NUM_DOCUMENTS/df]
	return (docid, word, tf_idf(word))
}
```
</section>

<div class="progress">
  <div class="progress-bar" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 70%;">
    <span class="sr-only">70% Complete</span>
  </div>
</div>

<section id="task4">
### Term Weights per Document
-ReduceToVector
This reduce task takes the output of the join and groups it by the document ids (`docid`).
Write the document id and the terms including their weight into the `WeightVector` you have created in the previous task.
Remember to uncomment the provided code.
Then implement the reduceGroup method accordingly in TfId.
</section>

## Congratulations!

<div class="progress">
  <div class="progress-bar progress-bar-success" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 100%;">
    <span class="sr-only">100% Complete</span>
  </div>
</div>
