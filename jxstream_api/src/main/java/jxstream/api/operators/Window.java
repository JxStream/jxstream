/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

import java.util.function.Function;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <I>
 */
public class Window<T, I> {

    /**
     *
     */
    public enum WindowType {

        TimeBased, TupleBased
    }
    private final int size;
    private final int advance;
    private final WindowType type;
    private final I defaultState;
    private final Function<T, Long> timeExtractor;

    private Window(int size, int advance, WindowType type, I defaultState, Function<T, Long> timer) {
        this.size = size;
        this.advance = advance;
        this.type = type;
        this.defaultState = defaultState;
        this.timeExtractor = timer;
    }

    /**
     *
     * @param <I>
     * @param size
     * @param advance
     * @param IdefaultState
     * @param timeExtractor
     * @return
     */
    public static <T,I> Window<T,I> createTimeBasedWindow(int size, int advance, I IdefaultState, Function<T, Long> timeExtractor) {
        return new Window(size, advance, WindowType.TimeBased, IdefaultState, timeExtractor);
    }

    /**
     *
     * @param <I>
     * @param size
     * @param advance
     * @param IdefaultState
     * @return
     */
    public static <T,I> Window<T,I> createTupleBasedWindow(int size, int advance, I IdefaultState) {
        return new Window(size, advance, WindowType.TupleBased, IdefaultState, null);
    }

    public int getSize() {
        return size;
    }

    public int getAdvance() {
        return advance;
    }

    public WindowType getType() {
        return type;
    }

    public I getDefaultState() {
        return defaultState;
    }

    public Function<T, Long> getTimeStamp() {
        return this.timeExtractor;
    }
}
