package org.systemsfords.p1.mr;

import java.util.List;

public interface Reducer {
	
	//The method every reducer UDF should implement
	public List<String> reduce(String key, List<String> values);
}
