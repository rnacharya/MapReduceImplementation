package org.systemsfords.p1.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

class TestFaultTolerance extends Thread{
    int N;
    String mode;
    TestFaultTolerance(int N, String mode){
        this.N = N;
        this.mode=mode;
    }
    @Override
    public void run(){
        try {

            Thread.sleep(10);
            while (true) {
            	
            		System.out.println("Tried to execute");
                    String pid=getProcessId();
                    String cmd = "kill -9 " + pid;
                    Process p = Runtime.getRuntime().exec(cmd);
            	    int exitCode=p.waitFor();
            	    if(exitCode==0) {
            	    	System.out.println("Command: "+cmd +" executed");
            	    	break;
            	    }
                    
                    
                        
                }
        }catch(Exception e){
            e.printStackTrace();

        }
    }
   
   public String runProcess(String cmd) throws IOException, InterruptedException {
	   String output="";
	   String line;
	    Process p = Runtime.getRuntime().exec(cmd);
	    BufferedReader input =
	            new BufferedReader(new InputStreamReader(p.getInputStream()));
	    while ((line = input.readLine()) != null) {
	        output+=line; //<-- Parse data here.
	    }
	    int exitCode=p.waitFor();
	    if(exitCode==0) {
	    	System.out.println("Command: "+cmd +" executed");
	    }
	    input.close();
	 return output;
   }
   
   public String getProcessId() throws IOException, InterruptedException {
	   String output="";
	    String line;
	    String className="MapperLibrary";
	    System.out.println("Mode: "+mode);
        if(mode.equals("reducer")) {
        	className="ReducerLibrary";
        }
	    String[] cmd = {
	    		"/bin/sh",
	    		"-c",
	    		"ps -ef | grep "+className,
	    		};
	    Process p = Runtime.getRuntime().exec(cmd);
	    BufferedReader input =
	            new BufferedReader(new InputStreamReader(p.getInputStream()));
	    while ((line = input.readLine()) != null) {
	        if(!line.contains("grep")) {
	        	output+=line;
	        }
	        
	    }
	    output=output.trim();
	    output=output.replaceAll("\\s+", " ");
	    
	    input.close();
	    return output.split(" ")[1];
	    
	    
   }
   
   public static void main(String[] args) throws Exception {
	   
	   String output="";
	    String line;
	    String className="MapperLibrary";
       
	    String[] cmd = {
	    		"/bin/sh",
	    		"-c",
	    		"ps -ef | grep "+className,
	    		};
	    Process p = Runtime.getRuntime().exec(cmd);
	    BufferedReader input =
	            new BufferedReader(new InputStreamReader(p.getInputStream()));
	    while ((line = input.readLine()) != null) {
	        if(!line.contains("grep")) {
	        	output+=line;
	        }
	        
	    }
	    output=output.trim();
	    output=output.replaceAll("\\s+", " ");
	    System.out.println("Grep command output: "+output);
	    input.close();
	    String pid= output.split(" ")[1];
       
	    Process pr= Runtime.getRuntime().exec("kill -9 " + pid);
	    int exitCode=pr.waitFor();
	    if(exitCode==0) {
	    	System.out.println("mapper".toUpperCase()+ " Process with id: " + pid +" killed");
	    }
       
	    
   }
   
}
