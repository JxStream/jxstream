/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public class PollerBlockWaitStrategy implements PollerWaitStrategy {

    Disruptor disruptor;
    private volatile long currentSeq;
    public PollerBlockWaitStrategy(Disruptor disruptor) {
        this.disruptor=disruptor;
        
        disruptor.handleEventsWith((EventHandler) (Object event, long sequence, boolean endOfBatch) -> {
                currentSeq=sequence;
                synchronized(this){
                    notifyAll();
                }
        });
        
    }
    
    

    @Override
    public void waitForData(long seq) {
        synchronized(this){            
                    if(seq>=currentSeq){
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            Logger.getLogger(PollerBlockWaitStrategy.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }     
                }
    }
    
}
