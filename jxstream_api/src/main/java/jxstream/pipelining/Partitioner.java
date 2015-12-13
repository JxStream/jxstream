/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import jxstream.api.JxPipe;
import jxstream.api.adapters.DisruptorSupplier;

/**
 *
 * @author andy and brian
 * @param <T> the type of tuples 
 */
 public class Partitioner<T> {
    final private PartitionedConsumer consumer;
    final private List<JxPipe<T>> downStreams;
    final JxPipe<T> upstream;
    final int numParts;

    /**
     *
     * @param upstream the upstream to split
     * @param flow a flow control that dictates how the tuples are sent to downstreams
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public Partitioner(JxPipe<T> upstream, FlowController flow) throws IOException {
        
        this.upstream=upstream;
        this.numParts=flow.getParts();
         consumer=new PartitionedConsumer(flow,1024);
        
        downStreams=new ArrayList<>();
        
        for(int i=0; i<flow.getParts();i++){
            downStreams.add(JxPipe.generate(new DisruptorSupplier<>(((Ring<T>)consumer.getRings().get(i)).getDisruptor())));
            
        }
    }
    
    /**
     * Starts the underlying pipeline,making it ready for streaming
     * @return a runnable for starting upstream
     */
    public Runnable getUpstreamSwitch(){
         for(int i=0; i<numParts;i++){
            ((Ring<T>)consumer.getRings().get(i)).start();
        }
        
       return new Runnable(){
             @Override
             public void run() {
                 upstream.forEach(consumer);
             }
       } ;
        
     }
    
    public List<JxPipe<T>> getPartitions(){
        return downStreams;
    }
     
    /**
     * Stops the underlying pipeline infrastructure 
     */
    public void shutDown(){
         for(int i=0; i<numParts;i++){
            ((Ring<T>)consumer.getRings().get(i)).stop();
        }
     }
}
