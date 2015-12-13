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
public class TimeAggregateWrapper<T,I, R> implements AggregateWrapper<T, AggregateTuple<R>> {

    private I defaultState;
    private int windowSize;
    private int advance;
    private Function<I,R> finisher;
    private I[] windows;//the window slots in a cycle to hold the aggregates
    private long[] startTimes;//the start time of each window slot
    
    private BiFunction<I, T, I> lambda;
    private Function<T, Long> getTimeStampFunction;
    private boolean firstTuple;
    

    public TimeAggregateWrapper(BiFunction<I, T, I> operator,Function<I,R> finisher, Window window, I defaultState) {
        if (window.getSize() % window.getAdvance() != 0) {
            throw new IllegalArgumentException("windowSize should be divisible by advance");
        }
        
        this.defaultState=defaultState;
        this.lambda = operator;
        this.finisher=finisher;
        this.getTimeStampFunction = window.getTimeStamp();
        this.windowSize = window.getSize();
        this.advance = window.getAdvance();
        windows = (I[]) new Object[this.windowSize / this.advance];
        startTimes = new long[windows.length];
        firstTuple = true;
    }
    

    @Override
     public  AggregateTuple<R> apply(T t) {
        R retData = null;
        long retWindowStart=0;
        
        long time = getTimeStampFunction.apply(t);
        
        for (int i = 0; i < windows.length; i++) {
            if (firstTuple) {//we initialize the start time since this is the first tuple
                if(i==0)
                    startTimes[i] = time;// + (advance + i);
                else
                    startTimes[i]=startTimes[i-1]+advance;
                windows[i]= defaultState;//(R) new Object(); 
            }

            if (startTimes[i] > time) {
                continue;//this tuple is not within this window
            }
            
            long diff = time - startTimes[i];

            if (diff < windowSize) {
                windows[i] = lambda.apply(windows[i], t);

            } else {
                retData =finisher.apply( windows[i]);//NOTE: Currently we return the latest aggregate if the time is due for multiple windows
                retWindowStart=startTimes[i];
                
                while (time - startTimes[i] >= windowSize) {
                    startTimes[i] = startTimes[i] + windowSize;
                }

                windows[i] = defaultState;//(R) new Object();//start all over
                windows[i] = lambda.apply(windows[i], t);

            }

        }

        if (firstTuple) {
            firstTuple = false;
        }

        return retData!=null? new AggregateTuple<>(retWindowStart,0,retData):null;

    }

}
