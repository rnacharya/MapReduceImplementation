package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MasterLibrary {
	String mapperUDF;
	String reducerUDF;

	public MasterLibrary(String mapperUDF, String reducerUDF) {
		this.mapperUDF = mapperUDF;
		this.reducerUDF = reducerUDF;
	}
	
	//TODO: read config file
	public void callMapperLibrary() {
		ProcessBuilder processBuilder = new ProcessBuilder();
		System.out.println(System.getProperty("user.dir")+"/target/mr-0.0.1-SNAPSHOT.jar");
        processBuilder.command("java", "-cp", System.getProperty("user.dir")+"/target/mr-0.0.1-SNAPSHOT.jar", "org.systemsfords.p1.mr.Mapper", mapperUDF);
        processBuilder.redirectErrorStream(true);
        try {

            Process process = processBuilder.start();

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            String path = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                path = line;
            }

            int exitCode = process.waitFor();
            System.out.println("\nExited with error code : " + exitCode);
            System.out.println(path);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}

}
