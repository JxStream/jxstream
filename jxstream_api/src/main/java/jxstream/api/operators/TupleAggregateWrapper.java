/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

import java.util.function.BiFunction;
import java.util.function.Function;


/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T> 
 * @param <I> 
 * @param <R>
 */
public class TupleAggregateWrapper<T,I,R> implements AggregateWrapper<T,AggregateTuple<R>>{
    
    private long tupleCount;
    private int windowSize;
    private I[] windows;
    private long[] windowStart;
    private BiFunction<I,T, I> lambda;
    private boolean firstTuple;
    private int advance;
    private I defaultState;
    private Function<I,R> finisher;
	
    public    TupleAggregateWrapper(BiFunction<I,T, I> lambda,Function<I,R> finisher, Window window, I defaultState){
		if (window.getSize() % window.getAdvance() != 0) {
            throw new IllegalArgumentException("windowSize should be divisible by advance");
        }
        this.defaultState = defaultState;
        this.lambda=lambda;
        this.finisher=finisher;
        this.windowSize= window.getSize();
        this.advance= window.getAdvance();
        windows=(I[])new Object[this.windowSize/this.advance];
        windowStart=new long[windows.length];
        firstTuple = true;
        
    }
    
    @Override
    public AggregateTuple<R> apply(T t) {
        R retData=null;
         long retWindowTuple=0;
        tupleCount++;
        for(int i=0;i<windows.length;i++){
            if(firstTuple){
                if(i==0)
                    windowStart[i]=tupleCount;//initialize the start of the window
                else
                    windowStart[i]=windowStart[i-1]+advance;
                
                windows[i]= defaultState;
            }
            
            if(windowStart[i]>tupleCount)
                continue;//tuple does not fall in this window
            
            
            long diff = tupleCount - windowStart[i];
            
            if (diff < windowSize) {
                windows[i] = lambda.apply(windows[i], t);

            } else {
                retData =finisher.apply( windows[i]);
                retWindowTuple=windowStart[i];
                windowStart[i] = windowStart[i] + windowSize;
                windows[i] = defaultState;//start all over
                windows[i] = lambda.apply(windows[i], t);

            }
             
        }
        

        if(firstTuple)
            firstTuple=false;
        
       return retData!=null? new AggregateTuple<>(0,retWindowTuple,retData):null;  
    }
    
}
