/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An aggregateWrapper based on the ring implementation. Basically the ring
 * represents a window. The ring is broken into slots which are equal to the
 * number of advances that make a full cycle(window_size/advance).
 *
 * @author brian mwambazi and andy philogene
 * @param <T> type for the input tuple
 * @param <I> type for the immediate state the per tuple operator will return
 * @param <R> type for the output data that will be wrapped in AggregateTuple<>
 */
public class TimeAggregateWrapperRing<T, I, R> implements AggregateWrapper<T, AggregateTuple<R>> {

    private final I defaultState;
    private final int windowSize;
    private final int advance;
    private int currentSlot;
    private final I[] windowSlotsRing;
    private final long[] slotStartTimes;
    private final BiFunction<I, T, I> perTupleOperator;
    private final BiFunction<I, I, I> combinerOperator;
    private final Function<I, R> finisher;
    private final Function<T, Long> getTimeStampFunction;
    private boolean firstTuple;
    private long initialTime;

    public TimeAggregateWrapperRing(BiFunction<I, T, I> perTupleOperator, BiFunction<I, I, I> combinerOperator,
            Function<I, R> finisher, Window window, I defaultState) {
        if (window.getSize() % window.getAdvance() != 0) {
            throw new IllegalArgumentException("windowSize should be divisible by advance");
        }
        this.defaultState = defaultState;
        this.perTupleOperator = perTupleOperator;
        this.getTimeStampFunction = window.getTimeStamp();
        this.combinerOperator = combinerOperator;
        this.finisher = finisher;
        this.windowSize = window.getSize();
        this.advance = window.getAdvance();

        //initialize the slots
        windowSlotsRing = (I[]) new Object[this.windowSize / this.advance];
        for (int i = 0; i < windowSlotsRing.length; i++) {
            windowSlotsRing[i] = defaultState;
        }

        slotStartTimes = new long[windowSlotsRing.length];

        firstTuple = true;
    }

    @Override
    public AggregateTuple<R> apply(T t) {
        R retValue = null;
        long retWindowStart = 0;
        long tupleTime = getTimeStampFunction.apply(t);

        if (firstTuple) {
            initState(tupleTime);
            firstTuple = false;
        }

        if (tupleTime < slotStartTimes[currentSlot] + advance) {
            windowSlotsRing[currentSlot] = perTupleOperator.apply(windowSlotsRing[currentSlot], t);
        } else {

            while (tupleTime >= slotStartTimes[currentSlot] + advance) {
                if (tupleTime >= initialTime + windowSize) {
                    retWindowStart = slotStartTimes[(currentSlot+1) % slotStartTimes.length];
                    retValue = getWindowData();//TODO this will capture the latest expired window if multiple
                                               //windows expire. Please change this
                }
                
                int prevSlot = currentSlot;
                currentSlot = clearAndGetNextSlot(currentSlot);
                slotStartTimes[currentSlot] = slotStartTimes[prevSlot] + advance;
            }

            windowSlotsRing[currentSlot] = perTupleOperator.apply(windowSlotsRing[currentSlot], t);
        }

        return retValue != null ? new AggregateTuple<>(0, retWindowStart, retValue) : null;
    }

    private void initState(long time) {
        initialTime = time;
        slotStartTimes[0] = time;
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
