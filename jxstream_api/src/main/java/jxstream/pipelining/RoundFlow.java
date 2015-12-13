/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

/**
 *
 * @author Andy Philogene & Brian Mwambazi
 * @param <T>
 */
public class RoundFlow<T> implements FlowController<T>{
	private final int numParts;
	private int count = 0;
	/**
     * Partition the stream in a ROund Robin fashion
     * @param parts number of partitions to split the string in
     * @throws IllegalArgumentException
     */
	public RoundFlow(int parts) throws IllegalArgumentException{
		if(parts<1){
            throw new IllegalArgumentException("Partition size cannot be less than 1");
        }
		this.numParts = parts;
	}
	
	@Override
	public int getParts(){
		return this.numParts;
	}
	
	@Override
	public Integer apply(T tuple) {
		if(count == Integer.MAX_VALUE)
			count = 0;
		return count++%numParts;
		
	}

	
	
	
}
