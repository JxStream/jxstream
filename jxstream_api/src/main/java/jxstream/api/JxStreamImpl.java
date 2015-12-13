/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collector;
import java.util.stream.Stream;
import jxstream.api.adapters.ConsumerConnectorsFactory;
import jxstream.api.adapters.SupplierConnectors;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.Window;
import jxstream.pipelining.FlowController;
import jxstream.pipelining.Partitioner;
import jxstream.pipelining.StreamMerger;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class JxStreamImpl<T> implements JxStream<T> {

    private List<JxPipe<T>> pipes;
    private PipelineEngine engine;

    //TODO  need to fix the constructors
    JxStreamImpl(Stream<T> stream) throws IOException {
        this(stream, null);

    }

    JxStreamImpl(Stream<T> stream, FlowController flow) throws IOException {

        this.engine = new PipelineEngine();

        if (flow != null && flow.getParts() > 1) {
            Partitioner partitioner = new Partitioner<>(new JxPipe(stream), flow);
            pipes = partitioner.getPartitions();
            this.engine.addSwitch(partitioner.getUpstreamSwitch());
        } else {
            pipes = new ArrayList<>();
            pipes.add(new JxPipe<>(stream));
        }

    }

    JxStreamImpl(JxDataSource<T> source, FlowController flow) throws IOException {
        this(source.getSupplier(), flow, source.getPipelineEngine());
    }

    JxStreamImpl(SupplierConnectors<T> supplier, FlowController flow, PipelineEngine engine) throws IOException {
        if (!Objects.isNull(engine)) {
            this.engine = engine;
        } else {
            this.engine = new PipelineEngine();
        }

        //lets put a hook to count upstream tuples
        final JxPipe srcPipe = new JxPipe<>(supplier).peek(x -> {
            this.engine.incUpstreamCount();
        });
        if (flow != null && flow.getParts() > 1) {

            Partitioner partitioner = new Partitioner<>(srcPipe, flow);
            pipes = partitioner.getPartitions();
            this.engine.addSwitch(partitioner.getUpstreamSwitch());
        } else {
            pipes = new ArrayList<>();
            pipes.add(srcPipe);
        }

    }
    
    JxStreamImpl(List<SupplierConnectors<T>> suppliers, PipelineEngine engine){
        if (!Objects.isNull(engine)) {
            this.engine = engine;
        } else {
            this.engine = new PipelineEngine();
        }
        pipes = new ArrayList<>();
        
        suppliers.stream().forEach((supplier) -> {
            pipes.add(JxPipe.generate(supplier)
                            .peek(x -> this.engine.incUpstreamCount()) // NOT thread safe
            );
        });
        
    }

    public JxStreamImpl(PipelineEngine engine, ArrayList<JxPipe<T>> pipes) {

        this.engine = engine;
        this.pipes = pipes;
    }

    private JxPipe mergeThreadedStreams(){
        JxPipe downStream = null;
        if(pipes.size() > 1){
            try {
                final StreamMerger<T> merger = new StreamMerger<>(pipes);
                //we add each switch to the engine
                merger.getSwitches().stream().forEach(s -> {
                    engine.addSwitch(s);
                });
                downStream = merger.getMergedStream();
            } catch (IOException ex) {
                Logger.getLogger(JxStreamImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else{
            downStream = pipes.get(0);
        }
        return downStream;
    }
    
    @Override
    public <R> JxStream<AggregateTuple<R>> 
        Aggregate(BiFunction<R, T, R> operator, Window<T, R> window) 
    {
        ArrayList<JxPipe<AggregateTuple<R>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.Aggregate(operator, window));
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <I, R> JxStream<AggregateTuple<R>> 
        Aggregate(BiFunction<I, T, I> operator, Function<I, R> finisher, Window<T, I> window) 
    {
        ArrayList<JxPipe<AggregateTuple<R>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.Aggregate(operator, finisher, window));
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <I,R> JxStream<AggregateTuple<R>> 
        aggregate(BiFunction<I, T, I> operator, BiFunction<I, I, I> combiner, Function<I, R> finisher, Window<T, I> window) 
    {
        ArrayList<JxPipe<AggregateTuple<R>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.aggregate(operator, combiner, finisher, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K, I, R> JxStream<AggregateTuple<Map<K, R>>> 
        aggregate(BiFunction<I, T, I> operator, Function<I, R> finisher, Function<T, K> groupBy, Window<T, I> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, R>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.aggregate(operator, finisher, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K, I, R> JxStream<AggregateTuple<Map<K, R>>> 
        aggregate(BiFunction<I, T, I> operator, BiFunction<I, I, I> combiner, Function<I, R> finisher, Function<T, K> groupBy, Window<T, I> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, R>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.aggregate(operator, combiner, finisher, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public JxStream<AggregateTuple<Double>> 
        Min(Function<T, Double> numberExtractor, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Double>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.Min(numberExtractor, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K> JxStream<AggregateTuple<Map<K, Double>>> 
        min(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, Double>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.min(numberExtractor, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public JxStream<AggregateTuple<Double>> 
        sumAggregate(Function<T, Double> numberExtractor, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Double>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.sumAggregate(numberExtractor, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K> JxStream<AggregateTuple<Map<K, Double>>> 
        sumAggregate(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, Double>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.sumAggregate(numberExtractor, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }
    
    @Override
    public JxStream<AggregateTuple<Double>> 
        Max(Function<T, Double> numberExtractor, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Double>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.Max(numberExtractor, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K> JxStream<AggregateTuple<Map<K, Double>>> 
        max(Function<T, Double> numberExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, Double>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.max(numberExtractor, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }
    
    @Override
    public JxStream<AggregateTuple<Double>> 
        average(Function<T, Double> fieldExtractor, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Double>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.average(fieldExtractor, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K> JxStream<AggregateTuple<Map<K, Double>>> 
        average(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, Double>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.average(fieldExtractor, groupBy, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public JxStream<AggregateTuple<Double>> 
        median(Function<T, Double> fieldExtractor, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Double>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.median(fieldExtractor, window) );
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <K> JxStream<AggregateTuple<Map<K, Double>>> 
        median(Function<T, Double> fieldExtractor, Function<T, K> groupBy, Window<T, Double> window) 
    {
        ArrayList<JxPipe<AggregateTuple<Map<K, Double>>>> newPipes = new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream.median(fieldExtractor, groupBy, window));
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Stream<T> toStream() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxStream<T> peek(Consumer<? super T> action) {
        ArrayList<JxPipe<T>> newPipes = new ArrayList<>();
        pipes.stream().forEach((e) -> {
            newPipes.add(e.peek(action));
        });

        this.pipes = newPipes;
        return this;
    }

    @Override
    public JxStream<T> limit(long maxSize) {
        ArrayList<JxPipe<T>> newPipes = new ArrayList<>();
        pipes.stream().forEach((e) -> {
            newPipes.add(e.limit(maxSize));
        });
        this.pipes = newPipes;
        return this;
    }

    @Override
    public JxStream<T> filter(Predicate<? super T> predicate) {
        ArrayList<JxPipe<T>> newPipes = new ArrayList<>();
        pipes.stream().forEach((e) -> {
            newPipes.add(e.filter(predicate));
        });
        this.pipes = newPipes;
        return this;
    }

    @Override
    public <R> JxStream<R> map(Function<? super T, ? extends R> mapper) {
        ArrayList<JxPipe<R>> newPipes = new ArrayList<>();
        pipes.stream().forEach((e) -> {
            newPipes.add(e.map(mapper));
        });
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public <R> JxStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        ArrayList<JxPipe<R>> newPipes = new ArrayList<>();
        pipes.stream().forEach((e) -> {
            newPipes.add(e.flatMap(mapper));
        });
        return new JxStreamImpl<>(this.engine, newPipes);
    }

    @Override
    public PipelineEngine forEach(final Consumer<? super T> action) {
        return foreachInternal(action,true);
    }
    
    @Override
    public PipelineEngine foreachParallel(ConsumerConnectorsFactory<? super T> factory){
        return foreachParallelInternal(factory,true);
    }

    private PipelineEngine foreachInternal(final Consumer<? super T> action,boolean endOfPipeline) {
        JxPipe downStream = null;
        if (pipes.size() > 1) {
            downStream=mergeThreadedStreams();
        } else {

            downStream = pipes.get(0);
        }
 
        final JxPipe dwn;
        if(endOfPipeline)
              dwn=downStream.peek(x->engine.incDownStreamCount());//we count the downstream tuples
        else
              dwn=downStream;

        engine.addSwitch(() -> {
            dwn.forEach(action);
        });

        return engine;
    }
    
    
    private PipelineEngine foreachParallelInternal(final ConsumerConnectorsFactory<? super T> factory,boolean endOfPipeline) {
        pipes.stream().forEach((p) -> { 
            engine.addSwitch(() -> {p.forEach(factory.createNewConsumer());});
        });

        return engine;
    }


    @Override
    public PipelineEngine forEachOrdered(Consumer<? super T> action) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Spliterator<T> spliterator() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isParallel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxStream<T> sequential() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxStream<T> parallel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxStream<T> unordered() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxStream<T> onClose(Runnable closeHandler) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void compute() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JxDataSource<T> subQuery() {
        JxDataSource<T> rtVal = null;
        try {
            JxQueue<T> q = JxQueueFactory.newTempQueue(JxQueueType.Disruptor, null, null,this.engine);
            rtVal = new JxDataSource<>(q.getSupplierFactory(), foreachInternal(q.getSink(),false));

        } catch (IOException ex) {
            Logger.getLogger(JxStreamImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rtVal;
    }

    @Override
    public PipelineEngine getPipelineEngine() {
        return forEach(x -> {
        });
    }
    
    @Override
    public JxStreamImpl<T> insertTupleCount(String name){
        ArrayList<JxPipe<T>> newPipes=new ArrayList<>();
        final long[]counts=new long[pipes.size()+1];//pos 0 is for start time
        int i=0;
        for (JxPipe<T> pipe : pipes) {
            final int pos=i;
            pipe=pipe.peek(x->{
                if(counts[0]==0){//this has potential of having many threads overwriting each other but we don't care 
                                //as at now since the error magin is so minimal
                    counts[0]=System.nanoTime();
                }
                counts[pos+1]++;
            });
            newPipes.add(pipe);
            i++;
        }
        this.engine.addNewCountStat(name,counts);
        return new JxStreamImpl<>(this.engine, newPipes);
    }
    
    @Override
    public JxStream<T> convergeParallelStreams(){//TODO maybe we should give an option of adding an ordering primitive
        if(pipes.size()==1)
            return this;//no need to converge the threads
        
        ArrayList<JxPipe<T>> newPipes=new ArrayList<>();
        JxPipe downStream = mergeThreadedStreams();
        newPipes.add(downStream);
        return new JxStreamImpl<>(this.engine, newPipes);
    }

}
