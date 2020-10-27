package org.systemsfords.p1.mr.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ReducerInvertedIndexUDF {
	
	public List<String> reduce(String key, List<String> values) {
		Set<String> set = new LinkedHashSet<String>(); 
		set.addAll(values); 
		values.clear(); 
		values.addAll(set); 
  
		return values;
	}
}
