/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 * @author andy and brian
 * @param <T> type of data thats being partitioned
 */
 class PartitionedConsumer<T> implements Consumer<T> {
    private final List<Ring<T>> rings;
    private final int numParts;
    private final FlowController<T> control;
    private  int ringSize;

	 PartitionedConsumer(FlowController c) {
		this(c, 1024);
	}
    
	 PartitionedConsumer(FlowController c, int internalQueueSize) {
        rings=new ArrayList<>();
        numParts=c.getParts();
		this.control = c;
    	this.ringSize=internalQueueSize;
        initilizeRings();
    }
    
    @Override
    public void accept(T tuple) {
        int index= this.control.apply(tuple);
        rings.get(index).add(tuple);
    }
    
    List<Ring<T>> getRings(){
        return rings;
    }

    private void initilizeRings() {
        for(int i=0;i<numParts;i++){
            Ring<T> ring=new Ring<>(ringSize);
            rings.add(ring);
        }
    }
    
    public void stop(){
        for(Ring r:rings){
            r.stop();
        }
            
    }
    
}

