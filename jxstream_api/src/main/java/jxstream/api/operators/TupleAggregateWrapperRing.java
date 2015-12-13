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
public class TupleAggregateWrapperRing <T, I, R> implements AggregateWrapper<T,AggregateTuple<R>> {

    private long tupleCount;
    private final I defaultState;
    private final int windowSize;
    private final int advance;
    private int currentSlot;
    private final I[] windowSlotsRing;
    private final long[] slotStartCnt;
    private final BiFunction<I, T, I> perTupleOperator;
    private final BiFunction<I, I, I> combinerOperator;
    private final Function<I, R> finisher;
    //private final Function<T, Long> getTimeStampFunction;
    private boolean firstTuple;
    private long initialTime;

    
    
    
    public TupleAggregateWrapperRing(BiFunction<I, T, I> perTupleOperator,BiFunction<I, I, I> combinerOperator,
            Function<I, R> finisher, Window window, I defaultState) {
        if (window.getSize() % window.getAdvance() != 0) {
            throw new IllegalArgumentException("windowSize should be divisible by advance");
        }
        this.defaultState = defaultState;
        this.perTupleOperator = perTupleOperator;
        //this.getTimeStampFunction = timeStampExtractor;
        this.combinerOperator=combinerOperator;
        this.finisher=finisher;
        this.windowSize = window.getSize();
        this.advance = window.getAdvance();
        
        //initialize the slots
        windowSlotsRing = (I[]) new Object[this.windowSize / this.advance];
        for (int i = 0; i < windowSlotsRing.length; i++) {
            windowSlotsRing[i] = defaultState;
        }
        
        slotStartCnt = new long[windowSlotsRing.length];

        firstTuple = true;
    }


    @Override
    public AggregateTuple<R> apply(T t) {
        R retValue = null;
        long rtWinStart=0;
        tupleCount++;
        //long tupleTime = getTimeStampFunction.apply(t);

        if (firstTuple) {
            initState(tupleCount);
            firstTuple = false;
        }

        if (tupleCount < slotStartCnt[currentSlot] + advance) {
            windowSlotsRing[currentSlot] = perTupleOperator.apply(windowSlotsRing[currentSlot], t);
        } else {
           
            
            if (tupleCount >= initialTime + windowSize)
            {
                rtWinStart=slotStartCnt[(currentSlot+1) % slotStartCnt.length];//the next slot has the current window start
                retValue = getWindowData();
            }
            
            
                int prevSlot=currentSlot;
                currentSlot = clearAndGetNextSlot(currentSlot);
                slotStartCnt[currentSlot] = slotStartCnt[prevSlot] + advance;
            

            windowSlotsRing[currentSlot] = perTupleOperator.apply(windowSlotsRing[currentSlot], t);

        }

        return retValue!=null? new AggregateTuple<>(0,rtWinStart,retValue):null;
    }

    private void initState(long time) {
        initialTime = time;
        slotStartCnt[0] = time;
    }

    private R getWindowData() {
        I combinedIntermediate = defaultState;
        for (I d : windowSlotsRing) {
            combinedIntermediate = combinerOperator.apply(combinedIntermediate, d);
        }
        return finisher.apply(combinedIntermediate);
    }

    private int clearAndGetNextSlot(int currentSlot) {
        int nextSlot = ++currentSlot % windowSlotsRing.length;
        windowSlotsRing[nextSlot] = defaultState;

        return nextSlot;
    }

    
}
