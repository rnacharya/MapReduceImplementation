package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javafx.util.Pair;

public class MapperEmployeeSalariesUDF {

	public List<Pair<String, String>> map(String key, String value) {
		List<Pair<String, String>> values = new ArrayList<Pair<String, String>>();
		try {
			String[] colValues = value.split(",");
			if (colValues.length == 10) {
				int age = Integer.parseInt(colValues[1]);
				int monthSalary = Integer.parseInt(colValues[4]);
				Pair<String, String> entry = new Pair(String.valueOf(age), String.valueOf(monthSalary));
				values.add(entry);
			}
		} catch (NumberFormatException e) {
//			e.printStackTrace();
		}
		return values;
	}
}
