--- 
layout: default 
title:  Join Document Frequency with Term Frequency
---




This task uses a new Contract: Match. It has two inputs, namely the outputs from the previous tasks.

In the `MatchStub`, the `match()` method has two parameters, a `PactRecord` from each input that matched on the term.

The following pseudo code describes what the join does.

```javascript
match( (word, df), (docid, word, tf)) {
	tf_idf(word) = tf * log [Util.NUM_DOCUMENTS/df]
	return (docid, word, tf_idf(word))
}
```