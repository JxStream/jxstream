/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import jxstream.api.JxPipe;
import jxstream.api.adapters.BatchedPoller;
import jxstream.api.adapters.DisruptorConsumer;
import jxstream.api.adapters.DisruptorSupplier;

/**
 * This class will merge the streams supplied into one stream. Each stream will be consumed asynchronously in
 * a separate thread and thus there are no ordering guarantees in the downstream.
 * @author brian and andy
 * @param <T>
 */
public class StreamMerger<T> {

    private final List<JxPipe<T>> streams;
    private final JxPipe<T> merged;
    private final Ring<T> mainRing;
    private final Executor threadpool;

    /**
     *
     * @param streams
     * @throws IOException
     */
    public StreamMerger(List<JxPipe<T>> streams) throws IOException {
        if (streams == null || streams.size() < 1) {
            throw new IllegalArgumentException();
        }
        this.streams = streams;

        mainRing = new Ring<>();
        threadpool = Executors.newCachedThreadPool();
        
        merged = createDownStream(mainRing.getDisruptor());

    }

    int streamCount() {
        return streams.size();
    }

    /**
     *  
     * @return the resulting down stream of the merge
     */
    public JxPipe<T> getMergedStream() {
        return merged;
    }

    private JxPipe<T> createDownStream(Disruptor<BatchedPoller.DataEvent<T>> disrupt) throws IOException {
        JxPipe<T> rtVal = JxPipe.generate(new DisruptorSupplier<>(disrupt));
        
        return rtVal;
    }

    /**
     * Starts the under laying pipeline  pipeline
     * @return 
     */
    public List<Runnable> getSwitches(){
         mainRing.getDisruptor().start();
         List<Runnable> rtval=new ArrayList<>();
         streams.stream().forEach((stream) -> {
             rtval.add(()-> {
                 stream.forEach(new DisruptorConsumer<>(mainRing.getDisruptor().getRingBuffer()));
             });
        });
            
            return rtval;
    }



}
