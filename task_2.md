--- 
layout: default 
title:  Term Frequency
---


Implement a second Mapper that also reads in the documents from the same file. 
This time, the output tuples shall look like this `(docid, word, frequency)`.

You should use the same splitter for the input line as in Task #1. But this time, use the `docid`. 
Use a HashMap to identify the frequency of each word in the document.