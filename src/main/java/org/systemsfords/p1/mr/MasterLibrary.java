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

        try {

            Process process = processBuilder.start();

            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();
            System.out.println("\nExited with error code : " + exitCode);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}

}
