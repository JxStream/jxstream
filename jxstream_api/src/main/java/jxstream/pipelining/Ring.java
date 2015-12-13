/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import jxstream.api.adapters.BatchedPoller;
import jxstream.api.adapters.DisruptorConsumer;

/**
 *
 * @author Andy Philogene & Brian Mwambazi
 */

public class Ring<T>{
    private final RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer;
    private DisruptorConsumer<T> consumer;
    Disruptor<BatchedPoller.DataEvent<T>> disruptor;
    public Ring(int size) {
        
        Executor executor = Executors.newCachedThreadPool();        
        disruptor = new Disruptor<>(BatchedPoller.DataEvent.<T>factory(), size, executor);
        ringBuffer = disruptor.getRingBuffer();
        consumer=new DisruptorConsumer<>(ringBuffer);//TODO this consumer should not be in here
    }

     Ring() {
        this(1024);
    }
    
    RingBuffer<BatchedPoller.DataEvent<T>> getRingBuffer(){
        return ringBuffer;
    }
    
    public Disruptor<BatchedPoller.DataEvent<T>> getDisruptor(){
        return disruptor;
    }
    
    
    void add(T item){
        consumer.accept(item);
    }
    
    public void start(){
        disruptor.start();
    }
    
    void stop(){
        disruptor.shutdown();
    }
    
}

