package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;

import org.systemsfords.p1.mr.Reducer;

public class ReducerUDF implements Reducer {

	/**
	 * Sums up all the values for each of the distinct words found in the input
	 */
	public List<String> reduce(String key, List<String> values) {
		int sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		List<String> nums = new ArrayList<String>();
		nums.add(String.valueOf(sum));
		return nums;
	}
}
