/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

/**
 *
 * This will make EventPoller much faster.Note: Not thread safe!!
 * 
 * @author brimzi
 * @param <T>
 */
public class BatchedPoller<T>
{

    private final EventPoller<BatchedPoller.DataEvent<T>> poller;
    private final int maxBatchSize;
    private final BatchedData<T> polledData;
    private final RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer;
    private PollerWaitStrategy waitStrategy;

    public BatchedPoller(RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer, int batchSize)
    {
        this(ringBuffer,batchSize,  x-> {Thread.yield();});//we default to yielding    
    }
    
    public BatchedPoller(RingBuffer<BatchedPoller.DataEvent<T>> ringBuffer, int batchSize,PollerWaitStrategy strategy)
    {
        this.ringBuffer=ringBuffer;
        this.poller = this.ringBuffer.newPoller();
        this.ringBuffer.addGatingSequences(poller.getSequence());

        if (batchSize < 1)
        {
            batchSize = 20;
        }
        this.maxBatchSize = batchSize;
        this.polledData = new BatchedData<>(this.maxBatchSize);
        waitStrategy=strategy;
    }

    public T poll() throws Exception
    {
        if (polledData.getMsgCount() > 0)
        {
            return polledData.pollMessage(); // we just fetch from our local
        }

        loadNextValues(poller, polledData); // we try to load from the ring
        return polledData.getMsgCount() > 0 ? polledData.pollMessage() : null;
    }

    private EventPoller.PollState loadNextValues(EventPoller<BatchedPoller.DataEvent<T>> poller, final BatchedData<T> batch)
                                                                                                              throws Exception
    {

        return poller.poll(new EventPoller.Handler<BatchedPoller.DataEvent<T>>()
        {
            @Override
            public boolean onEvent(BatchedPoller.DataEvent<T> event, long sequence, boolean endOfBatch) throws Exception
            {
                T item = event.copyOfData();
                return item != null ? batch.addDataItem(item) : false;
            }
        });
    }
    
    
    public void waitForNewData(){
        waitStrategy.waitForData(poller.getSequence().get()+1);
    }

    RingBuffer getRing() {
       return ringBuffer; 
    }

    public static class DataEvent<T>
    {

        T data;

        public static <T> EventFactory<BatchedPoller.DataEvent<T>> factory()
        {
            return new EventFactory<BatchedPoller.DataEvent<T>>()
            {

                @Override
                public BatchedPoller.DataEvent<T> newInstance()
                {
                    return new BatchedPoller.DataEvent<T>();
                }
            };
        }

        public T copyOfData()
        {
            // Copy the data out here. In this case we have a single reference
            // object, so the pass by
            // reference is sufficient. But if we were reusing a byte array,
            // then we
            // would need to copy
            // the actual contents.
            return data;
        }

        public void set(T d)
        {
            data = d;
        }

    }

    private static class BatchedData<T>
    {

        private int msgHighBound;
        private final int capacity;
        private final T[] data;
        private int cursor;

        @SuppressWarnings("unchecked")
        BatchedData(int size)
        {
            this.capacity = size;
            data = (T[]) new Object[this.capacity];
        }

        private void clearCount()
        {
            msgHighBound = 0;
            cursor = 0;
        }

        public int getMsgCount()
        {
            return msgHighBound - cursor;
        }

        public boolean addDataItem(T item) throws IndexOutOfBoundsException
        {
            if (msgHighBound >= capacity)
            {
                throw new IndexOutOfBoundsException("Attempting to add item to full batch");
            }

            data[msgHighBound++] = item;
            return msgHighBound < capacity;
        }

        public T pollMessage()
        {
            T rtVal = null;
            if (cursor < msgHighBound)
            {
                rtVal = data[cursor++];
            }
            if (cursor > 0 && cursor >= msgHighBound)
            {
                clearCount();
            }
            return rtVal;
        }
    }
}
