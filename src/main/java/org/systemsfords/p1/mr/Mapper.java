package org.systemsfords.p1.mr;

import java.util.List;



public interface Mapper {
	
	//The method every mapper UDF should implement
	public List<Pair<String, String>> map(String key, String value);
}
