/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import jxstream.api.JxPipe;
import jxstream.api.adapters.BatchedPoller;
import jxstream.api.adapters.DisruptorConsumer;
import jxstream.api.adapters.DisruptorSupplier;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.IntermediateTuple;
import jxstream.api.operators.Window;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class LoadBalancer<T> {
	private final DisruptorSupplier<T> source;
	private final DisruptorConsumer<T> dest;
	//private final JxPipe<T> pipe;
	private final int numThreads;
	private final RingBuffer<BatchedPoller.DataEvent<T>> threadBuffers[];
	private final DisruptorConsumer<T> threadConsumers[];
	private final Disruptor<BatchedPoller.DataEvent<T>> disruptors[];
	private final ExecutorService pool;
	private final int bufferSize = 2048;
	
	public LoadBalancer(DisruptorSupplier<T> source, DisruptorConsumer<T> dest, int numThreads){
		if(numThreads<1) throw new IllegalArgumentException("Pipeline threads must be > 0");
		this.source = source;
		this.dest = dest;
		this.numThreads = numThreads;
		this.threadBuffers = new RingBuffer[numThreads];
		this.threadConsumers = new DisruptorConsumer[numThreads];
		this.disruptors= new Disruptor[numThreads];
		for(int i=0; i<numThreads; i++){
			//Create the necessary resources for the ringbuffers
			Executor executor = Executors.newCachedThreadPool();
			this.disruptors[i] = new Disruptor(BatchedPoller.DataEvent.factory(), bufferSize, executor,
												ProducerType.SINGLE,  new YieldingWaitStrategy() );
			this.disruptors[i].start();
			this.threadBuffers[i] = this.disruptors[i].getRingBuffer();
			this.threadConsumers[i] = new DisruptorConsumer(this.threadBuffers[i]);
		}
		this.pool =  Executors.newFixedThreadPool(numThreads);
		int i=0;
		while(i<numThreads){
			final int j = i++;
			Runnable thread = () ->{
				//Create a Stream for each thread from its supplier to the destination Consumer
				try {
					String[] threadName= new String[1];
					threadName[0] = Thread.currentThread().getName();
					JxPipe<T> threadStream = JxPipe.generate(new DisruptorSupplier(threadBuffers[j]));
					System.out.println("PipeThread_"+threadName[0]);
					Window win = Window.createTupleBasedWindow(100, 20, new IntermediateTuple(0.0, 0.0));
					threadStream.average(x-> Double.valueOf(x.toString().split("_")[2]), win);
						//.Max(x-> {AggregateTuple<Double> t = (AggregateTuple)x;
						//		return t.getAggregate();} ,		null, win)
						//.peek(x->{System.out.println(x);})
                                                //.forEach(this.dest);
					//.map(x-> { String d = x.toString(); d = threadName[0]+"_"+d;
					//		return (T) d;})
						
				} catch (IOException ex) {
					Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null, ex);
					Thread.currentThread().interrupt();
				}
			};
			pool.execute(thread);
		}
	}
	
	public void start(){
		Integer whichThread[] = new Integer[1];
		whichThread[0] = 0;
		// THe balancer will choose the consumer to send data, in a circular fashion
		while(true){
			T data =  source.get();
			this.threadConsumers[whichThread[0]].accept(data);
			whichThread[0] =  ++whichThread[0] % numThreads;
		}
	}
	
	public void shutdown(){
		for(int i=0; i<numThreads; i++){
			this.disruptors[i].shutdown();
			this.pool.shutdown();
			this.threadBuffers[i] = null;
		}
	}
	
}
