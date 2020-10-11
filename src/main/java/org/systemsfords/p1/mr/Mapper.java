package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class Mapper {

	public static void main(String args[]) {
		String fileName = "public/sample.txt";
		String contentsFile = readFile(fileName);
		map("public/sample.txt", contentsFile);
	}
	
	public static void map(String key, String value) {
		String processedContents = value.replaceAll("\\p{Punct}", " ");
		String[] terms = processedContents.split("\\s+");
		try {
            PrintWriter writer = new PrintWriter(new FileWriter("public/tokenized.txt", false));
            for (String term : terms) {
    			writer.println(term + ", " + 1);
    		}
            writer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
	}
	
	public static String readFile(String fileName) {
        StringBuilder sb  = new StringBuilder();
        try {
            FileReader fr = new FileReader(fileName);
            BufferedReader br = new BufferedReader(fr);
            String s;
            while((s = br.readLine()) != null) {
                sb.append(s);
            }
            fr.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return sb.toString();
    }
}