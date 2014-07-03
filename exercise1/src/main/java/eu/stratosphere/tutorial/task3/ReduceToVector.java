package eu.stratosphere.tutorial.task3;

import java.util.Iterator;

import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

/**
 * ReduceToVector extends the GroupReduceFunction. 
 * <p>
 * Its main purpose is to specify how the processed input 
 * will be organized in the newly created data type Weight Vector
 * <p>
 */

public class ReduceToVector extends 
	GroupReduceFunction<Tuple3<Integer, String, Double>, WeightVector> {
	
//-----------------------------------------------------------------------------------------------------
//	-- Does not need to be modified --
//-----------------------------------------------------------------------------------------------------
	
	
	@Override
	public void reduce(Iterator<Tuple3<Integer, String, Double>> values,
			Collector<WeightVector> out) throws Exception {
		
		WeightVector vector = new WeightVector();
		Tuple3<Integer, String, Double> value = values.next();
		vector.setDocId(value.f0);
		vector.add(value.f1, value.f2);
		while (values.hasNext()){
			value = values.next();
			vector.add(value.f1, value.f2);
		}
		out.collect(vector);
	}

}
