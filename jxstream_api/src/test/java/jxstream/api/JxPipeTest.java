/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.Window;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author brimzi
 */
public class JxPipeTest {

    public JxPipeTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test of iterator method, of class JxPipe.
     */
    //@Test
    public void testIterator() {
        System.out.println("iterator");
        JxPipe instance = null;
        Iterator expResult = null;
        Iterator result = instance.iterator();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of spliterator method, of class JxPipe.
     */
    //@Test
    public void testSpliterator() {
        System.out.println("spliterator");
        JxPipe instance = null;
        Spliterator expResult = null;
        Spliterator result = instance.spliterator();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of isParallel method, of class JxPipe.
     */
    // @Test
    public void testIsParallel() {
        System.out.println("isParallel");
        JxPipe instance = null;
        boolean expResult = false;
        boolean result = instance.isParallel();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of sequential method, of class JxPipe.
     */
    //@Test
    public void testSequential() {
        System.out.println("sequential");
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.sequential();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of parallel method, of class JxPipe.
     */
    //@Test
    public void testParallel() {
        System.out.println("parallel");
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.parallel();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of unordered method, of class JxPipe.
     */
    //@Test
    public void testUnordered() {
        System.out.println("unordered");
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.unordered();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of onClose method, of class JxPipe.
     */
    //@Test
    public void testOnClose() {
        System.out.println("onClose");
        Runnable closeHandler = null;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.onClose(closeHandler);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class JxPipe.
     */
    //@Test
    public void testClose() {
        System.out.println("close");
        JxPipe instance = null;
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of collect method, of class JxPipe.
     */
    //@Test
    public void testCollect() {
        System.out.println("collect");
        JxPipe instance = null;
        Object expResult = null;
        Object result = instance.collect(null);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of toStream method, of class JxPipe.
     */
    //@Test
    public void testToStream() {
        System.out.println("toStream");
        JxPipe instance = null;
        Stream expResult = null;
        Stream result = instance.toStream();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of generate method, of class JxPipe.
     */
    //@Test
    public void testGenerate() {
        System.out.println("generate");
        JxPipe expResult = null;
        JxPipe result = JxPipe.generate(null);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of limit method, of class JxPipe.
     */
    //@Test
    public void testLimit() {
        System.out.println("limit");
        long maxSize = 0L;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.limit(maxSize);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of filter method, of class JxPipe.
     */
    //@Test
    public void testFilter() {
        System.out.println("filter");
        Predicate predicate = null;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.filter(predicate);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of peek method, of class JxPipe.
     */
    //@Test
    public void testPeek() {
        System.out.println("peek");
        Consumer action = null;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.peek(action);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of map method, of class JxPipe.
     */
    //@Test
    public void testMap() {
        System.out.println("map");
        Function mapper = null;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.map(mapper);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of flatMap method, of class JxPipe.
     */
    //@Test
    public void testFlatMap() {
        System.out.println("flatMap");
        Function mapper = null;
        JxPipe instance = null;
        JxPipe expResult = null;
        JxPipe result = instance.flatMap(mapper);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of forEach method, of class JxPipe.
     */
    //@Test
    public void testForEach() {
        System.out.println("forEach");
        Consumer action = null;
        JxPipe instance = null;
        instance.forEach(action);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of forEachOrdered method, of class JxPipe.
     */
    //@Test
    public void testForEachOrdered() {
        System.out.println("forEachOrdered");
        Consumer action = null;
        JxPipe instance = null;
        instance.forEachOrdered(action);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of Aggregate method, of class JxPipe.
     */
    @Test
    public void testAggregate_BiFunction_Window() {
        System.out.println("Aggregate BiFunction_Window Time");
        Stream<Integer> s = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        BiFunction<Integer,Integer,Integer> operator = (x,y) -> {return x+y;};
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Integer> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Integer>> res = instance.Aggregate(operator, time_window);
        List<Integer> l = new ArrayList<>();
        res.toStream().forEach(x -> l.add(x.getAggregate()));
        List<Integer> expected = Arrays.asList(10, 18, 26, 34);
        assertArrayEquals(l.toArray(), expected.toArray());

        System.out.println("Aggregate BiFunction_Window Tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Integer> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Integer>> res_tuple = instance_tuple.Aggregate(operator, tuple_window);
        List<Integer> l_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        List<Integer> expected_tuple = Arrays.asList(10, 18, 26, 34);
        assertArrayEquals(l_tuple.toArray(), expected_tuple.toArray());
    }

    /**
     * Test of Aggregate method, of class JxPipe.
     */
    @Test
    public void testAggregate_3args() {
        System.out.println("Aggregate 3args Time");
        Stream<Integer> s = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        BiFunction<Integer,Integer,Integer> operator = (x,y) -> {return x+y;};
        Function<Integer,Integer> finisher = x -> x+1;
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Integer> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Integer>> res = instance.Aggregate(operator, finisher, time_window);
        List<Integer> l = new ArrayList<>();
        res.toStream().forEach(x -> l.add(x.getAggregate()));
        List<Integer> expected = Arrays.asList(11, 19, 27, 35);
        assertArrayEquals(l.toArray(), expected.toArray());
        
        System.out.println("Aggregate 3args Tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Integer> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Integer>> res_tuple = instance_tuple.Aggregate(operator, finisher, tuple_window);
        List<Integer> l_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        assertArrayEquals(l_tuple.toArray(), expected.toArray());
    }

    /**
     * Test of aggregate method, of class JxPipe.
     */
    @Test
    public void testAggregate_4args_1() {
        System.out.println("Aggregate 4args_1 Time");
        Stream<Integer> s = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        BiFunction<Double,Integer,Double> operator = (x,y) -> {return x + y.doubleValue();};
        Function<Double,Double> finisher = x -> x+1;
        BiFunction<Double, Double, Double> combiner = (x,y) ->x+y;
        Window<Integer,Double> time_window = Window.createTimeBasedWindow(4, 2, 0.0, timeExtract);
        JxPipe<Integer> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Double>> res = instance.aggregate(operator, combiner, finisher, time_window);
        List<Double> result = new ArrayList<>();
        res.toStream().forEach(x -> result.add(x.getAggregate()));
        List<Double> expected = Arrays.asList(11.0, 19.0, 27.0, 35.0);
        assertArrayEquals(result.toArray(), expected.toArray());
        
        System.out.println("Aggregate 4args_1 Tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window<Integer,Double> tuple_window = Window.createTupleBasedWindow(4, 2, 0.0);
        JxPipe<Integer> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> res_tuple = instance_tuple.aggregate(operator,combiner, finisher, tuple_window);
        List<Double> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        assertArrayEquals(expected.toArray(), result_tuple.toArray());
    }

    /**
     * Test of aggregate method, of class JxPipe.
     */
    @Test
    public void testAggregate_4args_2() {
        System.out.println("Aggregate 4args_2 Time");
        Stream<Double> s = Stream.<Double>iterate(1.0, x -> x + 1.0).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        BiFunction<Integer,Double,Integer> operator = (x,y) -> {return x + y.intValue();};
        Function<Integer,Integer> finisher = x -> x+1;
        Function<Double, String> groupBy = x ->{
            if(x<=4.0)                return "1";
            else if(x>4.0 && x<=8.0)    return "2";
            else                    return "3";
        };
        Window<Double,Integer> time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Map<String,Integer>>> res = instance.aggregate(operator, finisher, groupBy, time_window);
        List<Map<String,Integer>> result = new ArrayList<>();
        res.toStream().forEach(x -> result.add(x.getAggregate()));
        HashMap<String,Integer> m1 = new HashMap<>(); m1.put("1", 11); 
        HashMap<String,Integer> m2 = new HashMap<>(); m2.put("1", 8) ; m2.put("2", 12); 
        HashMap<String,Integer> m3 = new HashMap<>(); m3.put("2", 27); 
        HashMap<String,Integer> m4 = new HashMap<>(); m4.put("2", 16); m4.put("3", 20);
        List<Map<String,Integer>> expected = Arrays.asList(m1, m2, m3, m4);
        //System.out.println("Expected: "+ expected.toString());
        //System.out.println("Returned: "+ result.toString());
        assertArrayEquals(result.toArray(), expected.toArray());
        
        System.out.println("Aggregate 4args_2 Tuple");
        Stream<Double> s_tuple = Stream.<Double>iterate(1.0, x -> x + 1.0).limit(11);
        Window<Double,Integer> tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String,Integer>>> res_tuple = instance_tuple.aggregate(operator, finisher, groupBy, tuple_window);
        List<Map<String,Integer>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        assertArrayEquals(expected.toArray(), result_tuple.toArray());
    }
    
    /**
     * Test of aggregate method, of class JxPipe.
     */
    @Test
    public void testAggregate_5args() {
        System.out.println("AggregateBroupBy_5args Time");
        Stream<Double> s = Stream.<Double>iterate(1.0, x -> x + 1.0).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        BiFunction<Integer,Double,Integer> operator = (x,y) -> {return x + y.intValue();};
        Function<Integer,Integer> finisher = x -> x+1;
        Function<Double, String> groupBy = x ->{
            if(x<=4.0)                return "1";
            else if(x>4.0 && x<=8.0)    return "2";
            else                    return "3";
        };
        Window<Double,Integer> time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Map<String,Integer>>> res = instance.aggregate(operator, (x,y)->x+y, finisher, groupBy, time_window);
        List<Map<String,Integer>> result = new ArrayList<>();
        res.toStream().forEach(x -> result.add(x.getAggregate()));
        HashMap<String,Integer> m1 = new HashMap<>(); m1.put("1", 11); 
        HashMap<String,Integer> m2 = new HashMap<>(); m2.put("1", 8) ; m2.put("2", 12); 
        HashMap<String,Integer> m3 = new HashMap<>(); m3.put("2", 27); 
        HashMap<String,Integer> m4 = new HashMap<>(); m4.put("2", 16); m4.put("3", 20);
        List<Map<String,Integer>> expected = Arrays.asList(m1, m2, m3, m4);
        //System.out.println("Expected: "+ expected.toString());
        //System.out.println("Returned: "+ result.toString());
        assertArrayEquals(result.toArray(), expected.toArray());
        
        System.out.println("AggregateBroupBy_5args Tuple");
        Stream<Double> s_tuple = Stream.<Double>iterate(1.0, x -> x + 1.0).limit(11);
        Window<Double,Integer> tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String,Integer>>> res_tuple = instance_tuple.aggregate(operator, (x,y)->x+y, finisher, groupBy, tuple_window);
        List<Map<String,Integer>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        //System.out.println("Expected: "+ expected.toString());
        //System.out.println("Returned: "+ result_tuple.toString());
        assertArrayEquals(expected.toArray(), result_tuple.toArray());
    }

    /**
     * Test of Min method, of class JxPipe.
     */
    @Test
    public void testMin_Function_Window() {
        System.out.println("Min Function_Window Tuple");
        Stream<Double> s_tuple = Stream.iterate(1.0, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0.0);
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> res_tuple = instance_tuple.Min(x->x, tuple_window);
        List<Double> l_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        List<Double> expected_tuple = Arrays.asList(1.0, 3.0, 5.0, 7.0);
        assertArrayEquals(l_tuple.toArray(), expected_tuple.toArray());

        System.out.println("Min Function_Window Time");
        Stream<Double> s = Stream.iterate(1.0, x -> x + 1).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Double>> res = instance.Min(x -> {
            return x;
        }, time_window);
        List<Double> l = new ArrayList<>();
        res.toStream().forEach(x -> l.add(x.getAggregate()));
        List<Double> expected = Arrays.asList(1.0, 3.0, 5.0, 7.0);
        assertArrayEquals(l.toArray(), expected.toArray());
    }

    /**
     * Test of min method, of class JxPipe.
     */
    @Test
    public void testMin_3args() {
        System.out.println("Min 3args Tuple");
        Stream<Double> s_tuple = Stream.iterate(1.0, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        Function<Double, String> groupBy = x ->{
            if(x<=4.0)                return "1";
            else if(x>4.0 && x<=8.0)    return "2";
            else                    return "3";
        };
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String, Double>>> res_tuple = instance_tuple.min(x -> x, groupBy, tuple_window);
        List<Map<String, Double>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        HashMap<String,Double> m1 = new HashMap<>(); m1.put("1", 1.0); 
        HashMap<String,Double> m2 = new HashMap<>(); m2.put("1", 3.0) ; m2.put("2", 5.0); 
        HashMap<String,Double> m3 = new HashMap<>(); m3.put("2", 5.0); 
        HashMap<String,Double> m4 = new HashMap<>(); m4.put("2", 7.0); m4.put("3", 9.0);
        List<Map<String,Double>> expected = Arrays.asList(m1, m2, m3, m4);
        assertArrayEquals(result_tuple.toArray(), expected.toArray());
        
        System.out.println("Min 3args Time");
        Stream<Double> s = Stream.iterate(1.0, x -> x + 1).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Map<String, Double>>> res = instance.min(x -> x, groupBy, time_window);
        List<Map<String, Double>> result = new ArrayList<>();
        res.toStream().forEach(x -> result.add(x.getAggregate()));
        assertArrayEquals(result_tuple.toArray(), expected.toArray());
    }

    /**
     * Test of Max method, of class JxPipe.
     */
    @Test
    public void testMax_Function_Window() {
        System.out.println("Max Function_Window Tuple");
        Stream<Double> s_tuple = Stream.iterate(1.0, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> res_tuple = instance_tuple.Max(x -> {
            return x;
        }, tuple_window);
        List<Double> l_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        List<Double> expected_tuple = Arrays.asList(4.0, 6.0, 8.0, 10.0);
        assertArrayEquals(expected_tuple.toArray(), l_tuple.toArray());

        System.out.println("Max Function_Window Time");
        Stream<Double> s = Stream.iterate(1.0, x -> x + 1).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Double>> res = instance.Max(x -> {
            return x;
        }, time_window);
        List<Double> l = new ArrayList<>();
        res.toStream().forEach(x -> l.add(x.getAggregate()));
        List<Double> expected = Arrays.asList(4.0, 6.0, 8.0, 10.0);
        assertArrayEquals(l.toArray(), expected.toArray());
    }

    /**
     * Test of max method, of class JxPipe.
     */
    @Test
    public void testMax_3args() {
        System.out.println("Max 3args Tuple");
        Stream<Double> s_tuple = Stream.iterate(1.0, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        Function<Double, String> groupBy = x ->{
            if(x<=4.0)                return "1";
            else if(x>4.0 && x<=8.0)    return "2";
            else                    return "3";
        };
        JxPipe<Double> instance_tuple = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String, Double>>> res_tuple = instance_tuple.max(x -> x, groupBy, tuple_window);
        List<Map<String, Double>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        HashMap<String,Double> m1 = new HashMap<>(); m1.put("1", 4.0); 
        HashMap<String,Double> m2 = new HashMap<>(); m2.put("1", 4.0) ; m2.put("2", 6.0); 
        HashMap<String,Double> m3 = new HashMap<>(); m3.put("2", 8.0); 
        HashMap<String,Double> m4 = new HashMap<>(); m4.put("2", 8.0); m4.put("3", 10.0);
        List<Map<String,Double>> expected = Arrays.asList(m1, m2, m3, m4);
        assertArrayEquals(result_tuple.toArray(), expected.toArray());
        
        System.out.println("Max 3args Time");
        Stream<Double> s = Stream.iterate(1.0, x -> x + 1).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Double> instance = new JxPipe<>(s);
        JxPipe<AggregateTuple<Map<String, Double>>> res = instance.max(x -> x, groupBy, time_window);
        List<Map<String, Double>> result = new ArrayList<>();
        res.toStream().forEach(x -> result.add(x.getAggregate()));
        assertArrayEquals(result_tuple.toArray(), expected.toArray());
    }

    /**
     * Test of sumAggregate method, of class JxPipe.
     */
    @Test
    public void testSumAggregate_Function_Window() {
        System.out.println("sumAggregate Function_Window Tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        JxPipe<Integer> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> result = instance.sumAggregate((Integer x) -> x.doubleValue(), tuple_window);
        List<Double> l_tuple = new ArrayList<>();
        result.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        List<Double> expected_tuple = Arrays.asList(10.0, 18.0, 26.0, 34.0);
        assertArrayEquals(l_tuple.toArray(), expected_tuple.toArray());

        System.out.println("sumAggregate Function_Window Time");
        Stream<Integer> s_time = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        JxPipe<Integer> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Double>> result_time = instance_time.sumAggregate((Integer x) -> x.doubleValue(), time_window);
        List<Double> l_time = new ArrayList<>();
        result_time.toStream().forEach(x -> l_time.add(x.getAggregate()));
        List<Double> expected_time = Arrays.asList(10.0, 18.0, 26.0, 34.0);
        assertArrayEquals(l_time.toArray(), expected_time.toArray());
    }

    /**
     * Test of sumAggregate method, of class JxPipe.
     */
    @Test
    public void testSumAggregate_3args() {
        System.out.println("sumAggregate 3args Tuple");
        Stream<Double> s_tuple = Stream.iterate(1.0, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0.0);
        Function<Double, String> groupBy = x ->{
            if(x<=4.0)                return "1";
            else if(x>4.0 && x<=8.0)    return "2";
            else                    return "3";
        };
        JxPipe<Double> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> res = instance.sumAggregate(x->x, groupBy, tuple_window);
        List<Double> result_tuple = new ArrayList<>();
        res.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        HashMap<String,Double> m1 = new HashMap<>(); m1.put("1", 10.0); 
        HashMap<String,Double> m2 = new HashMap<>(); m2.put("1", 7.0) ; m2.put("2", 11.0); 
        HashMap<String,Double> m3 = new HashMap<>(); m3.put("2", 26.0); 
        HashMap<String,Double> m4 = new HashMap<>(); m4.put("2", 15.0); m4.put("3", 19.0);
        List<Map<String,Double>> expected = Arrays.asList(m1, m2, m3, m4);
        assertArrayEquals(result_tuple.toArray(), expected.toArray());

        System.out.println("sumAggregate 3args Time");
        Stream<Double> s_time = Stream.iterate(1.0, x -> x + 1).limit(11);
        Function<Double, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0.0, timeExtract);
        JxPipe<Double> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Map<String, Double>>> res_time = instance_time.sumAggregate(x->x, groupBy, time_window);
        List<Map<String, Double>> result_time = new ArrayList<>();
        res_time.toStream().forEach(x -> result_time.add(x.getAggregate()));
        assertArrayEquals(result_time.toArray(), expected.toArray());
    }
    
    /**
     * Test of average method, of class JxPipe.
     */
    @Test
    public void testAverage_Function_Window() {
        System.out.println("Average Function_Window tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0.0);
        JxPipe<Integer> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> res_tuple = instance.average((Integer x) -> x.doubleValue(), tuple_window);
        List<Double> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        List<Double> expected = Arrays.asList(2.5, 4.5, 6.5, 8.5);
        Assert.assertArrayEquals(expected.toArray(), result_tuple.toArray());

        System.out.println("Average Function_Window time");
        Stream<Integer> s_time = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0.0, timeExtract);
        JxPipe<Integer> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Double>> res_time = instance_time.average((Integer x) -> x.doubleValue(), time_window);
        List<Double> result_time = new ArrayList<>();
        res_time.toStream().forEach(x -> result_time.add(x.getAggregate()));
        Assert.assertArrayEquals(expected.toArray(), result_time.toArray());
    }

    /**
     * Test of average method, of class JxPipe.
     */
    @Test
    public void testAverage_3args() {
        System.out.println("Average 3args tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        Function<Integer, String> groupBy = x ->{
            if(x<=4)                return "1";
            else if(x>4 && x<=8)    return "2";
            else                    return "3";
        };
        JxPipe<Integer> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String, Double>>> res_tuple = instance.average((Integer x) -> x.doubleValue(), groupBy, tuple_window);
        List<Map<String, Double>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        HashMap<String,Double> m1 = new HashMap<>(); m1.put("1", 2.5); 
        HashMap<String,Double> m2 = new HashMap<>(); m2.put("1", 3.5) ; m2.put("2", 5.5); 
        HashMap<String,Double> m3 = new HashMap<>(); m3.put("2", 6.5); 
        HashMap<String,Double> m4 = new HashMap<>(); m4.put("2", 7.5); m4.put("3", 9.5);
        List<Map<String,Double>> expected = Arrays.asList(m1, m2, m3, m4);
        Assert.assertArrayEquals(expected.toArray(), result_tuple.toArray());

        System.out.println("Average 3args tuple");
        Stream<Integer> s_time = Stream.iterate(1, x -> x + 1).limit(11);
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, (Integer x)->x.longValue());
        JxPipe<Integer> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Map<String, Double>>> res_time = instance_time.average((Integer x) -> x.doubleValue(), groupBy, time_window);
        List<Map<String, Double>> result_time = new ArrayList<>();
        res_time.toStream().forEach(x -> result_time.add(x.getAggregate()));
        Assert.assertArrayEquals(expected.toArray(), result_time.toArray());
    }

    /**
     * Test of median method, of class JxPipe.
     */
    @Test
    public void testMedian_Function_Window() {
        System.out.println("Median Function_Window tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0.0);
        JxPipe<Integer> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Double>> result = instance.median((Integer x) -> x.doubleValue(), tuple_window);
        List<Double> l_tuple = new ArrayList<>();
        result.toStream().forEach(x -> l_tuple.add(x.getAggregate()));
        List<Double> expected_tuple = Arrays.asList(2.5, 4.5, 6.5, 8.5);
        Assert.assertArrayEquals(expected_tuple.toArray(), l_tuple.toArray());

        System.out.println("Median Function_Window time");
        Stream<Integer> s_time = Stream.iterate(1, x -> x + 1).limit(11);
        Function<Integer, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0.0, timeExtract);
        JxPipe<Integer> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Double>> res_time = instance_time.average((Integer x) -> x.doubleValue(), time_window);
        List<Double> result_time = new ArrayList<>();
        res_time.toStream().forEach(x -> result_time.add(x.getAggregate()));
        List<Double> expected_time = Arrays.asList(2.5, 4.5, 6.5, 8.5);
        Assert.assertArrayEquals(expected_time.toArray(), result_time.toArray());
    }

    /**
     * Test of median method, of class JxPipe.
     */
    @Test
    public void testMedian_3args() {
        System.out.println("Median 3args tuple");
        Stream<Integer> s_tuple = Stream.iterate(1, x -> x + 1).limit(11);
        Window tuple_window = Window.createTupleBasedWindow(4, 2, 0);
        Function<Integer, String> groupBy = x ->{
            if(x<=4)                return "1";
            else if(x>4 && x<=8)    return "2";
            else                    return "3";
        };
        JxPipe<Integer> instance = new JxPipe<>(s_tuple);
        JxPipe<AggregateTuple<Map<String, Double>>> res_tuple = instance.median((Integer x) -> x.doubleValue(), groupBy, tuple_window);
        List<Map<String, Double>> result_tuple = new ArrayList<>();
        res_tuple.toStream().forEach(x -> result_tuple.add(x.getAggregate()));
        HashMap<String,Double> m1 = new HashMap<>(); m1.put("1", 2.5); 
        HashMap<String,Double> m2 = new HashMap<>(); m2.put("1", 3.5) ; m2.put("2", 5.5); 
        HashMap<String,Double> m3 = new HashMap<>(); m3.put("2", 6.5); 
        HashMap<String,Double> m4 = new HashMap<>(); m4.put("2", 7.5); m4.put("3", 9.5);
        List<Map<String,Double>> expected = Arrays.asList(m1, m2, m3, m4);
        Assert.assertArrayEquals(expected.toArray(), result_tuple.toArray());

        System.out.println("Median 3args tuple");
        Stream<Integer> s_time = Stream.iterate(1, x -> x + 1).limit(11);
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, (Integer x)->x.longValue());
        JxPipe<Integer> instance_time = new JxPipe<>(s_time);
        JxPipe<AggregateTuple<Map<String, Double>>> res_time = instance_time.median((Integer x) -> x.doubleValue(), groupBy, time_window);
        List<Map<String, Double>> result_time = new ArrayList<>();
        res_time.toStream().forEach(x -> result_time.add(x.getAggregate()));
        Assert.assertArrayEquals(expected.toArray(), result_time.toArray());
    }

}
