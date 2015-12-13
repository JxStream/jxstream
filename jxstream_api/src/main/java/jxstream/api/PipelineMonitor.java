/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public class PipelineMonitor {

    private enum States{
        None,
        Resume,
        Shutdown
    }
    private static final Map<String,PipelineEngine> pipelines = new HashMap<>();

    
    private static BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
    
     /**
     * Adds this pipeline to the monitor
     *
     * @param name name of this pipeline,this has to be unique
     * @param pipeline The engine to monitor
     */
    public static void register(String name,PipelineEngine pipeline) {
        register(name, pipeline, 0);
    }
    /**
     * Adds this pipeline to the monitor
     *
     * @param name name of this pipeline,this has to be unique
     * @param pipeline The engine to monitor
     * @param count the maximum tuple to calculate stats ( count<1: unbounded stream)
     */
    public static void register(String name,PipelineEngine pipeline, int count) {
        if(pipelines.containsKey(name))
            throw new IllegalArgumentException("Pipeline name already exist");
        pipeline.setLimit(count);
        pipelines.put(name,pipeline);
    }

    /**Monitors the registered pipelines.
     * This will block the calling thread.
     * @param reportInterval the time interval in seconds between reports in seconds
     */
    public static void monitor(int reportInterval) {
        HashMap<String, Boolean> shouldPrintStat = new HashMap<>();
        pipelines.entrySet().stream().forEach( (entry) -> {
            shouldPrintStat.put(entry.getKey(), Boolean.FALSE);
        });
        while(!Thread.interrupted()){
            StringBuilder builder=new StringBuilder();
            builder.append("\nReport at ").append(new Date());
            builder.append("\n=======================================================================");

            for (Map.Entry<String, PipelineEngine> entry : pipelines.entrySet()) {
                
                String name = entry.getKey();
                PipelineEngine engine = entry.getValue();
                if(!engine.isStarted()){
                    builder.append(String.format("\n%s not started\n", name));
                    continue;
                }
                if(engine.getLimit() < 1){ // For unbounded streams, keep printing
                    builder.append(String.format("\n Stats for %s ",name));
                    builder.append("\n-------------------------------------------------\n");
                    builder.append(engine.getStats());
                }else{ 
                    if( !engine.reachedLimit() ){
                        builder.append(String.format("\n Stats for %s ",name));
                        builder.append("\n-------------------------------------------------\n");
                        builder.append(engine.getStats());
                        
                    }else{ // stop printing stats after reaching limit
                        builder.append(String.format("\n Stats for %s ",name));
                        builder.append("\n-------------------------------------------------\n");
                        builder.append(engine.getStatsAtLimit());
                        builder.append("Test have reached MaxTupleCount");
                        pipelines.remove(name);  // Stats won't be printed for that engine anymore
                    }
                }
                
                
                
            }
            System.out.println(builder.toString());
            builder.setLength(0);
            
            switch(checkForUserInput()){
                case Resume:
                    
                    break;
                case Shutdown:
                    
                    break;
                default:
                    break;
            }
            
            try {
                Thread.sleep(reportInterval*1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(PipelineMonitor.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            if(pipelines.isEmpty())
                break;
        }
    }
    
    private static States checkForUserInput() {
        try {
            if(reader.ready()&& Objects.nonNull(reader.readLine())){
     
                reader=new BufferedReader(new InputStreamReader(System.in));
                System.out.println("You have entered interative mode so reporting has been paused");
                System.out.println("Type <x> and press enter\n"
                                 + "  r to resume reporting\n"
                                 + "  s to shut down the processing");
                String input=reader.readLine();
                switch(input){
                    case "r":
                        return States.Resume;

                    case "s":
                        return States.Shutdown;
                }

                
            }
        } catch (IOException ex) {
            Logger.getLogger(PipelineMonitor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return States.None;
    }

}
