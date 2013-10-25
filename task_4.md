--- 
layout: default 
title:  "Create a custom datatype: WeightVector"
---




Stratosphere comes with a bunch of build-in datatypes. For this assignment, you are going to implement your own. This task is a preparation for the last one. We want to store a `WeightVector`.
The datatype has a `add(String, double)` method to store the weight of each word.

The main task here is implementing the data de/serialization methods which are used before transferring the data through the network or to disk.

We recommend to store each `(String, double)` pair in two lists.
