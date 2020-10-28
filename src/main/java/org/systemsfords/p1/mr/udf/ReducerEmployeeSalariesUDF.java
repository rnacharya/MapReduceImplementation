package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.List;

public class ReducerEmployeeSalariesUDF {

	/**
	 * Computes the average salary of employees for each of the distinct ages found in the input
	 * @param key
	 * @param values
	 * @return
	 */
	public List<String> reduce(String key, List<String> values) {
		double sum = 0;
		for (String value : values) {
			sum += Integer.parseInt(value);
		}
		List<String> nums = new ArrayList<String>();
		double average = sum / values.size();
		nums.add(String.valueOf(average));
		return nums;
	}
}
