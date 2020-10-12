package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class App {

	public static void main(String[] args) throws IOException {

		MasterLibrary master = new MasterLibrary("org.systemsfords.p1.mr.udf.MapperUDF",
				"org.systemsfords.p1.mr.udf.ReducerUDF");
		master.callMapperLibrary();
		master.callReducerLibrary();
	}
}
