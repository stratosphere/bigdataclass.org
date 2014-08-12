---
layout: default_exercise
title:  "Basics: Word Count"
---

<section id="overview">
## Word Count explained
In big data, the word count example has come to be known as the equivalent of the well known Hello World! example.

We will first start with the Java implementation.

<section id="task1">
	
```java
public class WordCountExample {
    public static void main(String[] args) throws Exception {
```   

####Excecution environment
The `ExecutionEnvironment` is a major starting point. It is the basis of a Flink program.

```java
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
```   

####Data input
At this step, we set up the input data.
 
```java
        DataSet<String> text = env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?");
```

`env.fromElements` is one way to do so. 

<subsection>
Similarly, one may use `env.readCsvFile(filePath)` or also `env.readTextFile(filePath)`.
A slightly different yet more general implementation would be to create one's own method.

```java
DataSet<String> text = getTextDataSet(env);
```

Thus requiring...

```java
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return WordCountData.getDefaultTextLineDataSet(env);
		}
	}
```
where WordCountData, a default data set (used if no parameters are given to the program), is created in the `.util` folder. (see below)

```java
public class WordCountData {

	public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
		
		return env.fromElements(
	            "Who's there?",
	            "I think I hear them. Stand, ho! Who's there?",
				"...",
				"etc etc etc"
		);
	}
}
```
</subsection>   

####Core of the program

```java
        DataSet<Tuple2<String, Integer>> wordCounts = text
            .flatMap(new LineSplitter())
            .groupBy(0)
            .sum(1);
```
-`flatMap` calls a method (`LineSplitter`) that splits the input lines into pairs of this form (word,1).
For example,   
(house,1)   
(city,1)   
(dog,1)   
(house,1)   
(city,1)     
(family,1)   
(city,1)   
...

```java
    public static final class LineSplitter extends FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
```

-`groupBy` as the name implies it, puts together the previously created tuples according to the specified field.
In this case, field "0" wich corresponds to the word. Thus we get,  

(house,1)   
(house,1) 
 
(city,1)  
(city,1)   
(city,1)   

(dog,1)   
   
(family,1)   
...  

-Thanks to this grouping, `sum`'s only job is to perform a simple addition. Again depending on the specified field, in this case "1", the count.    

####Executing the program & showing the output

```java
        wordCounts.print();
        env.execute("Word Count Example");
    }
}
```
</section>

###Done!
<div class="progress">
  <div class="progress-bar progress-bar-success" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width: 100%;">
    <span class="sr-only">100% Complete</span>
  </div>
</div>   


####Worth noting...   
Flink offers a lot of flexibility in using tuples. 
The number of fields varies from 1 to 25. It allows nesting and types may be different.   

However, one is also free to use Plain Old Java Objects (POJOs) as shown below.   

```java
// some ordinary POJO
public class WC {
  public String word;
  public int count;
  // [...]
}

// ReduceFunction that sums Integer attributes of a POJO
public class WordCounter extends ReduceFunction<WC> {
  @Override
  public WC reduce(WC in1, WC in2) {
    return new WC(in1.word, in1.count + in2.count);
  }
}

// [...]
DataSet<WC> words = // [...]
DataSet<WC> wordCounts = words
                         // DataSet grouping with inline-defined KeySelector function
                         .groupBy(
                           new KeySelector<WC, String>() {
                             public String getKey(WC wc) { return wc.word; }
                           })
                         // apply ReduceFunction on grouped DataSet
                         .reduce(new WordCounter());
```   

For more details, please refer to the [Flink Java API guide](http://flink.incubator.apache.org/docs/0.6-SNAPSHOT/java_api_guide.html "Flink Java API guide")