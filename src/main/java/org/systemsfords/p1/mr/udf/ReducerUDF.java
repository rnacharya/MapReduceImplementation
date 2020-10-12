package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;

public class ReducerUDF {

	public static List<String> reduce(String key, List<String> values) {
		int sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		List<String> nums = new ArrayList<String>();
		nums.add(String.valueOf(sum));
		return nums;
	}
}
