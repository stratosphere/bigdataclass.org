--- 
layout: default 
title:  Overview
---

# Stratosphere Programming Assignment

In this programming assignment, you are going to program the [tf-idf](http://en.wikipedia.org/wiki/Tf%E2%80%93idf) for a small dataset in Stratosphere. Td-idf is an important weight for example for search engines.

## Set up

First, you need to install git, Maven 3 and Eclipse on your machine.

```
sudo apt-get install git maven eclipse
```

Next, clone the skeleton for the assignment.
```
git clone https://github.com/stratosphere/stratosphere-tutorial.git
cd stratosphere
```

Import this project into Eclipse, using the "Import -> Import as Maven Project" menu.
This could take a while as Maven is going to download all the dependencies.


##Task 1: Document Frequency

You are first going to calculate the document frequency. That is, in how many documents each distinct word occurs.
So if a document contains a word three times, it is counted only once for this document. But if another document contains the word as well, the overall frequency is two.

Besides this, you need to do some data cleansing on the input data. That is, accept only terms (words) that are alphanumerical. There is also a list of stopwords (provided by us) which should be rejected.

To achieve this, you need to implement a Mapper and a Reducer. The input file is a regular text file that contains a line for each document.
The schema of the input file looks like this:
```
docid, document contents
```
The Mapper is called with the documents. So each call of the `map()` function gets one line (e.g. a document).

You need to do the following in the Mapper: First, split the input line so that only alphanumerical words are accepted. Remember to remove the document id. 

Then, check if the individual terms are in the HashSet (`Util.STOP_WORDS`) of stopwords. You do not need to do additional extra checks because we already checked the input.

You should also use a [HashSet](http://docs.oracle.com/javase/7/docs/api/java/util/HashSet.html) to check if the word already occured for the given document. Remember, count each word only once here.

The last step is to emit all words with a frequency count of 1. Use a `PactRecord` with two fields. Set the word as the first field and the count (always 1) as the second field.
You have to use the build-in datatypes for `String` and `int` (`PactString` and `PactInteger`).
Use the output collector (second argument of `map()`) to emit the `PactRecord`


Use the provided `main()` method to test your code.


Task #1 is quite similar to the classical WordCount example, which is something like the "Hello World" of Big Data.

##Task 2: Term Frequency

Implement a second Mapper that also reads in the documents from the same file. 
This time, the output tuples shall look like this `(docid, word, frequency)`.

You should use the same splitter for the input line as in Task #1. But this time, use the `docid`. 
Use a HashMap to identify the frequency of each word in the document.


##Task 3: Join Document Frequency with Term Frequency

This task uses a new Contract: Match. It has two inputs, namely the outputs from the previous tasks.

In the `MatchStub`, the `match()` method has two parameters, a `PactRecord` from each input that matched on the term.

The following pseudo code describes what the join does.

```javascript
match( (word, df), (docid, word, tf)) {
	tf_idf(word) = tf * log [Util.NUM_DOCUMENTS/df]
	return (docid, word, tf_idf(word))
}
```


 
##Task 4: Create a custom datatype: WeightVector

Stratosphere comes with a bunch of build-in datatypes. For this assignment, you are going to implement your own. This task is a preparation for the last one. We want to store a `WeightVector`.
The datatype has a `add(String, double)` method to store the weight of each word.

The main task here is implementing the data de/serialization methods which are used before transferring the data through the network or to disk.

We recommend to store each `(String, double)` pair in two lists.


##Task 5: Reduce: WeightVector per Document.

This reduce task takes the output of the join and groups it by the document ids (`docid`).
Write the document id and the terms including their weight into the `WeightVector` you have created in the previous task.












