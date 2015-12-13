/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.IntermediateTuple;
import jxstream.api.operators.Window;
import jxstream.api.operators.AggregateWrapper;
import jxstream.api.operators.TimeAggregateWrapper;
import jxstream.api.operators.TimeAggregateWrapperRing;
import jxstream.api.operators.TupleAggregateWrapper;
import jxstream.api.operators.TupleAggregateWrapperRing;
import jxstream.api.adapters.SupplierConnectors;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class JxPipe<T> {

    private final Stream<T> stream;

    public JxPipe(Stream<T> stream) {
        this.stream = stream;
    }

    public JxPipe(SupplierConnectors<T> supplier) {
        this.stream = Stream.generate(supplier);
        //return this;
    }

    public static <T> JxPipe<T> generate(SupplierConnectors<T> supplier) {
        return new JxPipe(supplier);
    }

    public JxPipe<T> limit(long maxSize) {
        return new JxPipe<>(this.stream.limit(maxSize));
    }

    public JxPipe<T> filter(Predicate<? super T> predicate) {
        return new JxPipe<>(this.stream.filter(predicate));
    }

    /////////////////  Private methods on top of which the others are built 
    private <I, R> JxPipe<AggregateTuple<R>> 
        basedArrayAggregate(BiFunction<I, T, I> operator, Function<I, R> finisher, Window<T, I> window, I defaultState) 
    {
        AggregateWrapper<T, AggregateTuple<R>> wrapper = null;
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            wrapper = new TimeAggregateWrapper<>(operator, finisher, window,  defaultState);
        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
            wrapper = new TupleAggregateWrapper<>(operator,finisher, window,  defaultState);
        }
        Stream<AggregateTuple<R>> outStream = stream.map(wrapper).filter(x -> x != null);
        return new JxPipe<>(outStream);
    }
        
    private <I, R> JxPipe<AggregateTuple<R>> 
        basedRingAggregate(BiFunction<I, T, I> operator, BiFunction<I, I, I> combiner, Function<I, R> finisher, Window<T, I> window, I defaultState) 
    {
        AggregateWrapper<T, AggregateTuple<R>> wrapper = null;
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            wrapper = new TimeAggregateWrapperRing<>(operator, combiner, finisher, window, defaultState);
        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
            wrapper = new TupleAggregateWrapperRing<>(operator, combiner, finisher, window, defaultState);
        }
        Stream<AggregateTuple<R>> outStream = stream.map(wrapper).filter(x -> x != null);
        return new JxPipe<>(outStream);
    }
        
    /////////////////   General Aggregates  
    public <R> JxPipe<AggregateTuple<R>> 
        Aggregate(BiFunction<R, T, R> operator, Window<T, R> window) 
    {
//        AggregateWrapper<T, AggregateTuple<R>> wrapper = null;
//
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapper<>(operator, x -> x, window,  window.getDefaultState());
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapper<>(operator, x -> x, window,  window.getDefaultState());
//        }
//
//        Stream<AggregateTuple<R>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedArrayAggregate(operator, x->x, window, window.getDefaultState());
    }

    public <I, R> JxPipe<AggregateTuple<R>> 
        Aggregate(BiFunction<I, T, I> operator, Function<I, R> finisher, Window<T, I> window) 
    {
//        AggregateWrapper<T, AggregateTuple<R>> wrapper = null;
//
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapper<>(operator, finisher, window,  window.getDefaultState());
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapper<>(operator,finisher, window,  window.getDefaultState());
//        }
//
//        Stream<AggregateTuple<R>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedArrayAggregate(operator, finisher, window, window.getDefaultState());
    }

    public <I,R> JxPipe<AggregateTuple<R>> 
        aggregate(BiFunction<I, T, I> operator, BiFunction<I, I, I> combiner, Function<I, R> finisher, Window<T, I> window) 
    {
        return this.basedRingAggregate(operator, combiner, finisher, window, window.getDefaultState());
    }
        
    public <K, I, R> JxPipe< AggregateTuple<Map<K,R>> > 
        aggregate( BiFunction<I, T, I> operator, Function<I, R> finisher, Function<T,K> groupBy, Window<T, I> window) 
    {
        BiFunction<Map<K ,I>, T, Map<K, I>> grouping = (map, tuple) ->{
            if(map == null){
                map = new HashMap<>();
            }
            K key = groupBy.apply(tuple);
            if(map.containsKey(key)){
                map.put(key, operator.apply(map.get(key), tuple));
            }else{
                map.put(key, operator.apply(window.getDefaultState(), tuple) );
            }
            return map;
        };
        Function< Map<K,I>, Map<K, R> > finishing = map ->{
            Map<K, R> result = new HashMap<>();
            map.entrySet().forEach( e ->{
                result.put(e.getKey(), finisher.apply(e.getValue()));
            });
            map.clear(); // Free Memory
            return result;
        };
        Window<T, Map<K,I>> newWindow; // I need a new one because of wrong type inference from I to Map
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), null, window.getTimeStamp());
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), null);
        }
        return this.basedArrayAggregate(grouping, finishing, newWindow, null);
    }
    
    public <K, I, R> JxPipe< AggregateTuple<Map<K,R>> > 
        aggregate( BiFunction<I, T, I> operator, BiFunction<I, I, I>combiner, Function<I, R> finisher, Function<T, K> groupBy, Window<T, I> window) 
    {
        BiFunction<Map<K,I>, T, Map<K,I>> grouping = (map, tuple) ->{
            if(map == null){
                map = new HashMap<>();
            }
            K key = groupBy.apply(tuple);
            if(map.containsKey(key)){
                map.put(key, operator.apply(map.get(key), tuple));
            }else{
                map.put(key, operator.apply(window.getDefaultState(), tuple) );
            }
            //System.out.println("tuple: "+tuple+" op: "+map.toString());
            return map;
        };
        BiFunction<Map<K,I>, Map<K,I>, Map<K,I>> combining = (map1, map2) ->{
            if(map1 == null){
                map1 = new HashMap<>();
            }
            final Map<K,I> result = new HashMap<>(map1);
            map2.entrySet().forEach( e -> {
                K key = e.getKey();
                I val = e.getValue();
                if(result.containsKey(key)){
                    result.put(key, combiner.apply(result.get(key), val));
                }else{
                    result.put(key, val);
                }
            });
            //System.out.println("Map1: "+map1.toString()+" Map2: "+map2.toString()+ " Comb: "+result.toString());
            return result;
        };
        Function< Map<K,I>, Map<K, R> > finishing = map ->{
            Map<K, R> result = new HashMap<>();
            map.entrySet().forEach( e ->{
                result.put(e.getKey(), finisher.apply(e.getValue()));
            });
            map.clear(); // Free Memory
            return result;
        };
        
        Window<T, Map<K,I>> newWindow;
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), null, window.getTimeStamp());
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), null);
        }
        
        return this.basedRingAggregate(grouping, combining, finishing, newWindow, null);
    }
    
    ////////////////   Specific Aggregates 
    public JxPipe<AggregateTuple<Double>> 
        Min(Function<T, Double> numberExtractor, Window<T, Double> window) 
    {
        AggregateWrapper<T, AggregateTuple<Double>> wrapper = null;
        BiFunction<Double, T, Double> operator = (x, y) -> {
            Double minOver = numberExtractor.apply(y);
            return x < minOver ? x : minOver;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x<y ? x:y;
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapperRing<>(operator, (x, y) -> x < y ? x : y, x -> x, window, Double.MAX_VALUE);
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapperRing<>(operator, (x, y) -> x < y ? x : y, x -> x, window, Double.MAX_VALUE);//TODO make this work  
//        }
//
//        Stream<AggregateTuple<Double>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedRingAggregate(operator, combiner, x ->x, window, Double.MAX_VALUE);
    }

    public <K> JxPipe<AggregateTuple<Map<K,Double>>> 
        min( Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        BiFunction<Double, T, Double> operator = (x, y) -> {
            Double minOver = numberExtractor.apply(y);
            return x < minOver ? x : minOver;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x<y ? x:y;
        // I need a new window to set the default state as Max value (Since we ignore the user input on these cases)
        Window<T, Double> newWindow; 
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), Double.MAX_VALUE, window.getTimeStamp());
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), Double.MAX_VALUE);
        }
        return this.aggregate(operator, combiner, x->x, groupBy, newWindow);
    }
    
    public JxPipe<AggregateTuple<Double>> 
        Max(Function<T, Double> numberExtractor, Window<T, Double> window) 
    {
        AggregateWrapper<T, AggregateTuple<Double>> wrapper = null;
        BiFunction<Double, T, Double> operator = (x, y) -> {
            Double maxOver = numberExtractor.apply(y);
            return x > maxOver ? x : maxOver;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x>y ? x:y;
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapperRing<>(operator, (x, y) -> x > y ? x : y, x -> x, window, Double.MIN_VALUE);
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapperRing<>(operator, (x, y) -> x > y ? x : y, x -> x, window, Double.MIN_VALUE);//TODO make this work  
//        }
//        Stream<AggregateTuple<Double>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedRingAggregate(operator, combiner, x->x, window, Double.MIN_VALUE);
    }

    public <K> JxPipe< AggregateTuple<Map<K, Double>> > 
        max(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        BiFunction<Double, T, Double> operator = (x, y) -> {
            Double maxOver = numberExtractor.apply(y);
            return x > maxOver ? x : maxOver;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x>y ? x:y;
        Window<T, Double> newWindow; 
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), Double.MIN_VALUE, window.getTimeStamp());
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), Double.MIN_VALUE);
        }
        return this.aggregate(operator, combiner, x->x, groupBy, newWindow);
    }
    
    public JxPipe<AggregateTuple<Double>> 
        sumAggregate(Function<T, Double> numberExtractor, Window<T,Double> window) 
    {
        BiFunction<Double, T, Double> operator = (x, y) -> {
            double total = x;
            double tupleValue = numberExtractor.apply(y);
            total += tupleValue;
            return total;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x+y ;
//        AggregateWrapper<T, AggregateTuple<Double>> wrapper = null;
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapperRing<>(operator, (x, y) -> x + y, x -> x, window, 0.0);
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapperRing<>(operator, (x, y) -> x + y, x -> x, window, 0.0);
//        }
//
//        Stream<AggregateTuple<Double>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedRingAggregate(operator, combiner, x->x, window, 0.0);
    }

    public <K> JxPipe<AggregateTuple<Map<K, Double>>> 
        sumAggregate(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T,Double> window) 
    {
        BiFunction<Double, T, Double> operator = (x, y) -> {
            double total = x;
            double tupleValue = numberExtractor.apply(y);
            total += tupleValue;
            return total;
        };
        BiFunction<Double,Double,Double> combiner = (x, y) -> x+y ;
        return this.aggregate(operator, combiner, x->x, groupBy, window);
    }
        
    public JxPipe<AggregateTuple<Double>> 
        average(Function<T, Double> fieldExtractor, Window window) 
    {
        BiFunction<IntermediateTuple, T, IntermediateTuple> operator = (x, y) -> {
            IntermediateTuple i;
            if (x.aggregate == 0.0 && x.state == 0.0) {
                i = new IntermediateTuple(0.0, 0.0);
            } else {
                i = x;
            }
            Double tupleValue = fieldExtractor.apply(y);
            i.aggregate += tupleValue;
            i.state += 1;
            return i;
        };
        BiFunction<IntermediateTuple, IntermediateTuple, IntermediateTuple> combiner = (x, y) -> {
            double total = x.aggregate + y.aggregate;
            double count = x.state + y.state;
            return new IntermediateTuple(total, count);
        };
        Function<IntermediateTuple, Double> finisher = x -> x.aggregate / x.state;
//        AggregateWrapper<T, AggregateTuple<Double>> wrapper = null;
//        if (window.getType().equals(Window.WindowType.TimeBased)) {
//            wrapper = new TimeAggregateWrapperRing<>(operator, combiner, finisher, window, new IntermediateTuple(0.0, 0.0));
//        } else if (window.getType().equals(Window.WindowType.TupleBased)) {
//            wrapper = new TupleAggregateWrapperRing<>(operator, combiner, finisher, window, new IntermediateTuple(0.0, 0.0));//TODO make this work    
//        }
//        Stream<AggregateTuple<Double>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedRingAggregate(operator, combiner, finisher, window, new IntermediateTuple(0.0, 0.0));
    }

    public <K> JxPipe<AggregateTuple<Map<K, Double>>> 
        average(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T,Double> window) 
    {
        BiFunction<IntermediateTuple, T, IntermediateTuple> operator = (x, y) -> {
            IntermediateTuple i;
            if (x.aggregate == 0.0 && x.state == 0.0) {
                i = new IntermediateTuple(0.0, 0.0);
            } else {
                i = x;
            }
            Double tupleValue = fieldExtractor.apply(y);
            i.aggregate += tupleValue;
            i.state += 1;
            return i;
        };
        BiFunction<IntermediateTuple, IntermediateTuple, IntermediateTuple> combiner = (x, y) -> {
            double total = x.aggregate + y.aggregate;
            double count = x.state + y.state;
            return new IntermediateTuple(total, count);
        };
        Function<IntermediateTuple, Double> finisher = x -> x.aggregate / x.state;
        Window<T, IntermediateTuple> newWindow; 
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), new IntermediateTuple(0.0, 0.0), window.getTimeStamp());
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), new IntermediateTuple(0.0, 0.0));
        }
        return this.aggregate(operator, combiner, finisher, groupBy, newWindow);
    }

    public JxPipe<AggregateTuple<Double>> 
        median(Function<T, Double> fieldExtractor, Window<T, Double> window) 
    {
        BiFunction<IntermediateTuple, T, IntermediateTuple> operator = (x, y) -> {
            IntermediateTuple i;
            if (x.elements.get(0) == 0.0 && x.elements.get(1) == 0.0) {
                i = new IntermediateTuple(new ArrayList<>());
            } else {
                i = x;
            }
            Double tupleValue = fieldExtractor.apply(y);
            i.elements.add(tupleValue);
            return i;
        };
        Function<IntermediateTuple, Double> finisher = x -> {
            Collections.sort(x.elements);
            if (x.elements.size() % 2 != 0) {
                return x.elements.get(x.elements.size() / 2);
            } else {
                double aggregate = x.elements.get(x.elements.size() / 2);
                double state = x.elements.get((x.elements.size() / 2) - 1); // Elements start from 0, but size has an extra one.
                return (aggregate + state) / 2.0;
            }
        };
        Window<T,IntermediateTuple> newWindow;
//        AggregateWrapper<T, AggregateTuple<Double>> wrapper = null;
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), 
                    new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))), window.getTimeStamp() );
//            wrapper = new TimeAggregateWrapper<>(operator, finisher, window, new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))));
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), 
                    new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))) );
//            wrapper = new TupleAggregateWrapper<>(operator, finisher, window, new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))));//TODO make this work    
        }
//
//        Stream<AggregateTuple<Double>> outStream = stream.map(wrapper).filter(x -> x != null);
//        return new JxPipe<>(outStream);
        return this.basedArrayAggregate(operator, finisher, newWindow, newWindow.getDefaultState() );
    }

    public <K> JxPipe<AggregateTuple<Map<K,Double>>> 
        median(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        BiFunction<IntermediateTuple, T, IntermediateTuple> operator = (x, y) -> {
            IntermediateTuple i;
            if (x.elements.get(0) == 0.0 && x.elements.get(1) == 0.0) {
                i = new IntermediateTuple(new ArrayList<>());
            } else {
                i = x;
            }
            Double tupleValue = fieldExtractor.apply(y);
            i.elements.add(tupleValue);
            return i;
        };
        Function<IntermediateTuple, Double> finisher = x -> {
            Collections.sort(x.elements);
            if (x.elements.size() % 2 != 0) {
                return x.elements.get(x.elements.size() / 2);
            } else {
                double aggregate = x.elements.get(x.elements.size() / 2);
                double state = x.elements.get((x.elements.size() / 2) - 1); // Elements start from 0, but size has an extra one.
                return (aggregate + state) / 2.0;
            }
        };
        Window<T,IntermediateTuple> newWindow;
        if (window.getType().equals(Window.WindowType.TimeBased)) {
            newWindow = Window.createTimeBasedWindow(window.getSize(), window.getAdvance(), 
                    new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))), window.getTimeStamp() );
        } else{
            newWindow = Window.createTupleBasedWindow(window.getSize(), window.getAdvance(), 
                    new IntermediateTuple(new ArrayList<>(Arrays.asList(0.0, 0.0))) );
        }
        return this.aggregate(operator, finisher, groupBy, newWindow);
    }
        
    public Iterator<T> iterator() {
        return stream.iterator();
    }

    public Spliterator<T> spliterator() {
        return stream.spliterator();
    }

    public boolean isParallel() {
        return stream.isParallel();
    }

    public JxPipe<T> sequential() {
        return new JxPipe<>(stream.sequential());
    }

    public JxPipe<T> parallel() {
        return new JxPipe<>(stream.parallel());
    }

    public JxPipe<T> unordered() {
        return new JxPipe<>(stream.unordered());
    }

    public JxPipe<T> onClose(Runnable closeHandler) {
        return new JxPipe<>(stream.unordered());
    }

    public void close() {
        stream.close();
    }

    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return stream.collect(collector);
    }

    public Stream<T> toStream() {
        return stream;
    }

    public JxPipe<T> peek(Consumer<? super T> action) {
        return new JxPipe<>(stream.peek(action));
    }

    public <R> JxPipe<R> map(Function<? super T, ? extends R> mapper) {
        return new JxPipe<>(stream.map(mapper));
    }

    public <R> JxPipe<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new JxPipe<>(stream.flatMap(mapper));
    }

    public void forEach(Consumer<? super T> action) {
        stream.forEach(action);
    }

    public void forEachOrdered(Consumer<? super T> action) {
        stream.forEachOrdered(action);
    }

}
