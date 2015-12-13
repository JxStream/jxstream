/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class DisruptorSupplier<T> implements SupplierConnectors<T> {
    private final BatchedPoller<T> poller;
    public DisruptorSupplier(RingBuffer<BatchedPoller.DataEvent<T>> ring) throws IOException {
        poller=new BatchedPoller<>(ring,30);

    }
    public DisruptorSupplier(Disruptor<BatchedPoller.DataEvent<T>> disruptor) throws IOException {
        poller=new BatchedPoller<>(disruptor.getRingBuffer(),30,new PollerBlockWaitStrategy(disruptor));

    }

    @Override
    public T get() {
        try {
            T rtVal=poller.poll();
            
            while(rtVal==null){
                poller.waitForNewData();
                rtVal=poller.poll();
            }
                
            return rtVal;
            
        } catch (Exception ex) {
            Logger.getLogger(DisruptorSupplier.class.getName()).log(Level.SEVERE, null, ex);
        }        
        return null;
    }
    
    
    @Override
    public  SupplierConnectors<T> getCopyWithFullMsgView(){
        try {
            return new DisruptorSupplier(poller.getRing());
        } catch (IOException ex) {
            Logger.getLogger(DisruptorSupplier.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException("Error occurred while intializing disruptor",ex);
        }
    }
    
    @Override
    public boolean supportsCopyWithFullMsgView(){
        return true;
    }

}
