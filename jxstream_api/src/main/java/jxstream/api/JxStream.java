/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.Stream;
import jxstream.api.adapters.ConsumerConnectorsFactory;
import jxstream.api.adapters.SupplierConnectors;
import jxstream.api.adapters.SupplierConnectorsFactory;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.Window;
import jxstream.pipelining.HashedFlow;
import jxstream.pipelining.RoundFlow;

/**
 *
 * @author brian and andy
 * @param <T>
 */
public interface  JxStream<T> extends BaseStream<T,  JxStream<T>> {

    /**
     *
     * @param <T>
     * @param supplier
     * @return a JxStream of type T
     * @throws java.io.IOException
     */
    public static <T>  JxStream<T> generate(SupplierConnectors<T> supplier) throws IOException {
        return new JxStreamImpl(supplier,null,null);
    }
    
    /**
     * This create parallel streams under the hood.The number of streams will be equal to the number of cores 
     * available to the JVM 
     * @param <T>
     * @param supplier
     * @return a JxStream of type T
     * @throws java.io.IOException
     */
    public static <T>  JxStream<T> generateParallel(SupplierConnectors<T> supplier) throws IOException {
        int cores=Runtime.getRuntime().availableProcessors();
        return generateParallel(supplier,cores);
    }
    
    /**
     * This create parallel streams under the hood. Since the data source is a supplier object a Single Stream is first used
     * to as upstream then split into many streams.
     * @param <T> The type of tuples
     * @param supplier the source of the stream
     * @param num number of parallel streams
     * @return a JxStream of type T
     * @throws IOException
     */
    public static <T>  JxStream<T> generateParallel(SupplierConnectors<T> supplier,int num) throws IOException { 
            return new JxStreamImpl(supplier,new RoundFlow(num) ,null);
    }
    
    /**
     *
     * @param <T>
     * @param factory Factory that holds the implementation of a supplier
     * @return
     * @throws IOException
     */
    public static <T>  JxStream<T> generate(SupplierConnectorsFactory<T> factory) throws IOException {
        return generateParallel(factory, 1);
    }
    
    /**
     * This create parallel streams under the hood. The number of streams will be equal to the number of cores 
     * available to the JVM. Using the passed in factory each thread has an independent supplier
     * thereby making this a more optimized parallel stream.
     * @param <T>
     * @param factory Factory that holds the implementation of a supplier
     * @return
     * @throws IOException
     */
    public static <T>  JxStream<T> generateParallel(SupplierConnectorsFactory<T> factory) throws IOException {
        int cores=Runtime.getRuntime().availableProcessors();
        return generateParallel(factory, cores);
    }
    
    /**
     * This create parallel streams under the hood.Using the passed in factory each thread has an independent supplier
     * thereby making this a more optimized parallel stream.
     * @param <T>
     * @param factory Factory that holds the implementation of a supplier
     * @param num
     * @return
     * @throws IOException
     */
    public static <T>  JxStream<T> generateParallel(SupplierConnectorsFactory<T> factory,int num) throws IOException {

            List<SupplierConnectors<T>> connectors=new ArrayList<>();
          
            for (int i = 0; i < num; i++) {
                connectors.add(factory.createNewSupplier());
            }
            
            return new JxStreamImpl(connectors,null);
    }
    
    /**
     *
     * @param <T>
     * @param queue
     * @return a JxStream of type T
     * @throws java.io.IOException
     */
    public static <T>  JxStream<T> generate(JxDataSource<T> queue) throws IOException {
        return new JxStreamImpl(queue,null);
    }
    
    /**
     *
     * @param <T>
     * @param queue
     * @return
     */
    public static <T>  JxStream<T> generateParallel(JxDataSource<T> queue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     *
     * @param <T>
     * @param queue
     * @param num
     * @return
     */
    public static <T>  JxStream<T> generateParallel(JxDataSource<T> queue, int num) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /**
     *Computes the whole topology
     */
    void compute();
    
    /**
     *
     * @return
     */
    JxDataSource<T> subQuery();

    /**
     * This method aggregate the tuples according to the parameters.
     * If the stream was parallelized, it will be merge into one stream, 
     * before computing the aggregate, for consistency purposes.
     * @param <R>
     * @param operator
     * @param window
     * @return
     */
    <R>  JxStream<AggregateTuple<R>> 
        Aggregate(BiFunction<R, T, R> operator, Window<T,R> window);

    /**
     * This method aggregate the tuples according to the parameters.
     * If the stream was parallelized, it will be merge into one stream, 
     * before computing the aggregate, for consistency purposes.
     * @param <I>
     * @param <R>
     * @param operator
     * @param finisher
     * @param window
     * @return
     */
    <I, R>  JxStream<AggregateTuple<R>> 
        Aggregate(BiFunction<I, T, I> operator, Function<I, R> finisher, Window<T,I> window);

    /**
     *
     * @param <I>
     * @param <R>
     * @param operator
     * @param combiner
     * @param finisher
     * @param window
     * @return
     */
    <I,R> JxStream<AggregateTuple<R>> 
        aggregate(BiFunction<I, T, I> operator, BiFunction<I, I, I> combiner, Function<I, R> finisher, Window<T, I> window);
    
    /**
     *
     * @param <K> Type of the Key to group by
     * @param <I>
     * @param <R>
     * @param operator
     * @param finisher
     * @param groupBy
     * @param window
     * @return
     */
    <K, I, R> JxStream< AggregateTuple<Map<K,R>> > 
        aggregate( BiFunction<I, T, I> operator, Function<I, R> finisher, Function<T,K> groupBy, Window<T, I> window); 

    /**
     *
     * @param <K> Type of the Key to group by
     * @param <I>
     * @param <R>
     * @param operator
     * @param combiner
     * @param finisher
     * @param groupBy
     * @param window
     * @return
     */
    <K, I, R> JxStream< AggregateTuple<Map<K,R>> > 
        aggregate( BiFunction<I, T, I> operator, BiFunction<I, I, I>combiner, Function<I, R> finisher, Function<T, K> groupBy, Window<T, I> window);

    /**
     *
     * @param numberExtractor
     * @param window
     * @return
     */
     JxStream<AggregateTuple<Double>> 
        Min(Function<T, Double> numberExtractor, Window<T,Double> window);

    /**
     *
     * @param <K> Type of the Key to group by
     * @param numberExtractor
     * @param groupBy
     * @param window
     * @return
     */
    <K> JxStream<AggregateTuple<Map<K,Double>>> 
        min( Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window);
    /**
     *
     * @param numberExtractor
     * @param window
     * @return
     */
     JxStream<AggregateTuple<Double>> 
        Max(Function<T, Double> numberExtractor, Window<T,Double> window);
     
    /**
     *
     * @param <K> Type of the Key to group by
     * @param numberExtractor
     * @param groupBy
     * @param window
     * @return
     */
    <K> JxStream< AggregateTuple<Map<K, Double>> > 
        max(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window);

    /**
     *
     * @param numberExtractor
     * @param window
     * @return
     */
     JxStream<AggregateTuple<Double>> 
        sumAggregate(Function<T, Double> numberExtractor, Window<T,Double> window);
     
    /**
     *
     * @param <K> Type of the Key to group by
     * @param numberExtractor
     * @param groupBy
     * @param window
     * @return
     */
    <K> JxStream<AggregateTuple<Map<K, Double>>> 
        sumAggregate(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T,Double> window);

    /**
     *
     * @param fieldExtractor
     * @param window
     * @return
     */
     JxStream<AggregateTuple<Double>> 
        average(Function<T, Double> fieldExtractor, Window<T,Double> window);
     
    /**
     *
     * @param <K> Type of the Key to group by
     * @param fieldExtractor
     * @param groupBy
     * @param window
     * @return
     */
    <K> JxStream<AggregateTuple<Map<K, Double>>> 
        average(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T,Double> window);

    /**
     *
     * @param fieldExtractor a function to extract the number to be used
     * @param window definition of the window
     * @return a JxStream of type AggregateTuple< Double>
     */
     JxStream<AggregateTuple<Double>> 
        median(Function<T, Double> fieldExtractor, Window<T,Double> window);
     
    /**
     *
     * @param <K> Type of the Key to group by
     * @param fieldExtractor
     * @param groupBy
     * @param window
     * @return
     */
    <K> JxStream<AggregateTuple<Map<K,Double>>> 
        median(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T, Double> window);

    /**
     *
     * @param <R>
     * @param <A>
     * @param collector
     * @return
     */
    <R, A> R collect(Collector<? super T, A, R> collector);

    /**
     *
     * @return
     */
    Stream<T> toStream();

    /**
     *
     * @param action
     * @return
     */
     JxStream<T> peek(Consumer<? super T> action);

    /**
     * Limit a stream to a maximum tuple count to processs.
     * If the stream is parallelized, the same limit will be applied to each thread 
     * @param maxSize
     * @return a JxStream of type T
     */
    JxStream<T> limit(long maxSize);

    /**
     *
     * @param predicate
     * @return
     */
    JxStream<T> filter(Predicate<? super T> predicate);
    
    /**
     *
     * @param <R>
     * @param mapper
     * @return
     */
    <R>  JxStream<R> map(Function<? super T, ? extends R> mapper);
    
    /**
     *
     * @param <R>
     * @param mapper
     * @return
     */
    <R>  JxStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper);
    
    /**
     *
     * @param action
     * @return 
     */
    PipelineEngine forEach(Consumer<? super T> action);
    
     /**
     * This is a parallel version of {@link forEach} in that when called on parallel streams,the streams are 
     * never merged to perform the action.Thus eliminating the overhead of merging the stream
     * If the under laying stream is not parallel it will execute just like the {@link forEach}
     * @param factory A factory that produces consumers
     * @return A {@link PipelineEngine} object
     */
    PipelineEngine foreachParallel(ConsumerConnectorsFactory<? super T> factory);
    /**
     *
     * @param action
     * @return 
     */
    PipelineEngine forEachOrdered(Consumer<? super T> action);
    
    /**
     *
     * @return
     */
    PipelineEngine getPipelineEngine();
    
    /**
     * This will create a tuple count meter that can be later monitored using the {@link PipelineMonitor}
     * @param name of this 
     * @return
     */
    JxStreamImpl<T> insertTupleCount(String name);
    
    
     /**
     * This will merge a parallel stream into one stream.Calling this on a single stream has no effect.
     * @return A merged stream
     */
    JxStream<T> convergeParallelStreams();
    
}
