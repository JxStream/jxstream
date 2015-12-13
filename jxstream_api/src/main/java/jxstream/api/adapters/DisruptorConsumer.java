/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.lmax.disruptor.RingBuffer;
import java.util.function.Consumer;


/**
 *
 * @author brimzi
 * @param <T>
 */
public class DisruptorConsumer<T> implements ConsumerConnectors<T> {

    private final RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer;

    public DisruptorConsumer(RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void accept(T t) {
        long sequence = ringBuffer.next();  
        try {
            BatchedPoller.DataEvent<T> event = ringBuffer.get(sequence); 
            event.set(t); 
        } finally {
            ringBuffer.publish(sequence);
        }
    }

}
