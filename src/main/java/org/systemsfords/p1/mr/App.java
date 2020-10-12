package org.systemsfords.p1.mr;

import java.io.IOException;

public class App {

	public static void main(String[] args) throws IOException {
		
//		String mapperUDFClassName=args[0];
//		String reducerUDFClassName=args[1];
//		String outputFile=args[2];
		MasterLibrary master = new MasterLibrary("org.systemsfords.p1.mr.udf.MapperUDF", "org.systemsfords.p1.mr.udf.ReducerUDF", "outputFile.txt");
		master.masterEntry();
	}
}

