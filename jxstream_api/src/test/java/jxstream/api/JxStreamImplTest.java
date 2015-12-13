/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.Window;
import jxstream.pipelining.HashedFlow;
import jxstream.pipelining.RoundFlow;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Andy Philogene & Brian Mwambazi
 */
public class JxStreamImplTest {

    public JxStreamImplTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of filter method, of class JxStreamImpl.
     */
    //@Test
    public void testFilter() {
        System.out.println("filter");
        Predicate predicate = null;
        JxStreamImpl instance = null;
        JxStream expResult = null;
        JxStream result = instance.filter(predicate);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of map method, of class JxStreamImpl.
     *
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     */
    @Test
    public void testMap() throws IOException, InterruptedException {
        System.out.println("map single pipeline ");
        Function mapper = x -> x;
        Stream<Integer> it = Stream.iterate(0, x -> x + 2).limit(10);
        JxStreamImpl instance = new JxStreamImpl(it);
        List<Integer> r = new ArrayList<>();
        JxStream<Integer> result = instance.map(mapper);
        result.limit(10).forEach(x -> r.add(x)).start();
        Thread.sleep(1000);
        List<Integer> exp = Arrays.asList(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        Assert.assertArrayEquals(exp.toArray(), r.toArray());

        System.out.println("map multiple pipeline ");
        Stream<Integer> it1 = Stream.iterate(0, x -> x + 2).limit(10);
        HashedFlow<Integer> flow = new HashedFlow<>(2, x -> x);
        JxStreamImpl instance1 = new JxStreamImpl(it1, flow);
        List<Integer> r1 = new ArrayList<>();
        JxStreamImpl<Integer> result1 = (JxStreamImpl) instance1.map(mapper);
        result1.limit(10).forEach(x -> r1.add(x)).start();
        Thread.sleep(1000);
        List<Integer> exp1 = Arrays.asList(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        Assert.assertArrayEquals(exp1.toArray(), r1.toArray());
    }

    /**
     * Test of flatMap method, of class JxStreamImpl.
     */
    @Test
    public void testFlatMap() throws IOException, InterruptedException {
        System.out.println("FlatMap single pipeline ");
        Stream<Integer> it = Stream.iterate(0, x -> x + 2).limit(10);
        JxStreamImpl instance = new JxStreamImpl(it);
        List<Integer> r = new ArrayList<>();
        JxStream<Integer> result = instance.flatMap(x -> Stream.of(x));
        result.forEach(x -> r.add(x)).start();
        Thread.sleep(1000);
        List<Integer> exp = Arrays.asList(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        Assert.assertArrayEquals(exp.toArray(), r.toArray());

        System.out.println("FlatMap Multiple pipeline ");
        Stream<Integer> it1 = Stream.iterate(0, x -> x + 2).limit(10);
        JxStreamImpl instance1 = new JxStreamImpl(it1);
        List<Integer> r1 = new ArrayList<>();
        JxStreamImpl<Integer> result1 = (JxStreamImpl) instance1.flatMap(x -> Stream.of(x));
        result1.limit(10).forEach(x -> r1.add(x)).start();
        Thread.sleep(1000);
        List<Integer> exp1 = Arrays.asList(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        Assert.assertArrayEquals(exp1.toArray(), r1.toArray());
    }

    /**
     * Test of Aggregate method, of class JxStreamImpl.
     * @throws java.io.IOException
     */
    @Test
    public void testAggregate_3args() throws IOException, InterruptedException {
        System.out.println("Aggregate single pipeline, Time window");
        Function<Integer, Long> timeExtract = x -> x.longValue();
        Window time_window = Window.createTimeBasedWindow(4, 2, 0, timeExtract);
        Stream<Integer> s = Stream.iterate(1, x -> x + 1).limit(11);
        JxStreamImpl<Integer> instance = new JxStreamImpl<>(s);
        JxStream<AggregateTuple<Integer>> res = instance.<Integer, Integer>Aggregate((x, y) -> {
            return x + y;
        }, x ->x ,  time_window);
        List<Integer> r = new ArrayList<>();
        res.forEach(x -> r.add(x.getAggregate())).start();
        Thread.sleep(1000);
        List<Integer> expected = Arrays.asList(10, 18, 26, 34);
        assertArrayEquals(r.toArray(), expected.toArray());
    }

    /**
     * Test of forEach method, of class JxStreamImpl.
     */
    //@Test
    public void testForEach() {
        System.out.println("forEach");
        Consumer action = null;
        JxStreamImpl instance = null;
        instance.forEach(action);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of forEachOrdered method, of class JxStreamImpl.
     */
    //@Test
    public void testForEachOrdered() {
        System.out.println("forEachOrdered");
        Consumer action = null;
        JxStreamImpl instance = null;
        instance.forEachOrdered(action);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of iterator method, of class JxStreamImpl.
     */
    //@Test
    public void testIterator() {
        System.out.println("iterator");
        JxStreamImpl instance = null;
        Iterator expResult = null;
        Iterator result = instance.iterator();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of spliterator method, of class JxStreamImpl.
     */
    //@Test
    public void testSpliterator() {
        System.out.println("spliterator");
        JxStreamImpl instance = null;
        Spliterator expResult = null;
        Spliterator result = instance.spliterator();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of isParallel method, of class JxStreamImpl.
     */
    //@Test
    public void testIsParallel() {
        System.out.println("isParallel");
        JxStreamImpl instance = null;
        boolean expResult = false;
        boolean result = instance.isParallel();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of sequential method, of class JxStreamImpl.
     */
    //@Test
    public void testSequential() {
        System.out.println("sequential");
        JxStreamImpl instance = null;
        JxStream expResult = null;
        JxStream result = instance.sequential();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of parallel method, of class JxStreamImpl.
     */
    //@Test
    public void testParallel() {
        System.out.println("parallel");
        JxStreamImpl instance = null;
        JxStream expResult = null;
        JxStream result = instance.parallel();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of unordered method, of class JxStreamImpl.
     */
    //@Test
    public void testUnordered() {
        System.out.println("unordered");
        JxStreamImpl instance = null;
        JxStream expResult = null;
        JxStream result = instance.unordered();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of onClose method, of class JxStreamImpl.
     */
    //@Test
    public void testOnClose() {
        System.out.println("onClose");
        Runnable closeHandler = null;
        JxStreamImpl instance = null;
        JxStream expResult = null;
        JxStream result = instance.onClose(closeHandler);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class JxStreamImpl.
     */
    //@Test
    public void testClose() {
        System.out.println("close");
        JxStreamImpl instance = null;
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of compute method, of class JxStreamImpl.
     */
    //@Test
    public void testCompute() {
        System.out.println("compute");
        JxStreamImpl instance = null;
        instance.compute();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of subQuery method, of class JxStreamImpl.
     */
    //@Test
    public void testSubQuery() throws IOException, InterruptedException {
        System.out.println("subQuery");

        Stream<Integer> it = Stream.iterate(1, x -> x + 1).limit(10);
        JxStreamImpl instance2 = new JxStreamImpl(it);
        JxDataSource<Integer> q = instance2.subQuery();

        Stream<Integer> result = Stream.generate(q.getSupplier());

        List<Integer> r = new ArrayList<>();
        q.getPipelineEngine().start();
        result.limit(10).forEach(x -> r.add(x));
        Thread.sleep(1000);
        List<Integer> exp = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Assert.assertArrayEquals(exp.toArray(), r.toArray());
    }

}
