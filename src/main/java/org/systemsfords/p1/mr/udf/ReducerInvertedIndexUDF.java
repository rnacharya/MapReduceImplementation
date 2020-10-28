package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ReducerInvertedIndexUDF {
	
	/**
	 * Values contains all the doc ids in which a words occurs in. Remove the duplicate doc ids and return.
	 * @param key
	 * @param values
	 * @return
	 */
	public List<String> reduce(String key, List<String> values) {
		Set<String> set = new LinkedHashSet<String>(); 
		set.addAll(values); 
		values.clear(); 
		values.addAll(set); 
  
		return values;
	}
}
