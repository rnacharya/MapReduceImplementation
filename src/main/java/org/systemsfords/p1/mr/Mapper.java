package org.systemsfords.p1.mr;

public interface Mapper {
	
	//The method every mapper UDF should implement
	public String map(String key, String value);
}
