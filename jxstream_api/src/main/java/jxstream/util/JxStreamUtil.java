/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import jxstream.api.JxPipe;

/**
 *
 * @author brian and andy
 */
public class JxStreamUtil {

    /**
     * Compares if two streams contain the same items.This method will consume the supplied streams
     * and thus the streams should be finite
     * @param <T>
     * @param stream1
     * @param stream2
     * @return true if the contents are the same
     */
    public static <T>boolean compare(JxPipe<T> stream1, JxPipe<T> stream2) {
        
        List<T> list1=stream1.collect(Collectors.toList());
        
        List<T> list2=stream2.collect(Collectors.toList());
        
        return Arrays.equals(list1.toArray(), list2.toArray());
        
    }
    
    
    
    /**
     * Compares if two streams contain the same items ignoring the order.This method will consume the supplied streams
     * and thus the streams should be finite
     * @param <T>
     * @param stream1
     * @param stream2
     * @return true if the contents are the same
     */
    public static <T extends Comparable> boolean compareIgnoreOrder(JxPipe<T> stream1, JxPipe<T> stream2) {
        List<T> list1=stream1.collect(Collectors.toList());
        
        List<Comparable> list2=stream2.collect(Collectors.toList());
        
        Collections.sort(list1);
        Collections.sort(list2);
        
        return Arrays.equals(list1.toArray(), list2.toArray());
    }

    
    
}
