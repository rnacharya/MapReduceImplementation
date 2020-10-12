package org.systemsfords.p1.mr;

import java.util.List;

public interface Reducer {
	public List<String> reduce(String key, List<String> values);
}
