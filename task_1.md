--- 
layout: default 
title:  Document Frequency
---


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
