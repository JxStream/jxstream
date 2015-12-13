/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.util.function.Function;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class HashedFlow<T> implements FlowController<T>{
	private final Function<T,Integer> hashFunction;
	private final int numParts;

	 /**
     * Partition the stream based on hashed lambda operation
     * @param parts number of partitions to split the string in
     * @param hasher a hashing function that produces a hash key to use in partitioning
     * @throws IllegalArgumentException
     */
	public HashedFlow(int parts, Function<T,Integer> hash) throws IllegalArgumentException{
		if(parts<1){
            throw new IllegalArgumentException("Partition size cannot be less than 1");
        }
		this.numParts = parts;
		this.hashFunction = hash;
	}
	
	@Override
	public int getParts() {
		return numParts;
	}

	@Override
	public Integer apply(T tuple) {
		return Math.abs(hashFunction.apply(tuple)) % numParts;
	}
	
}
