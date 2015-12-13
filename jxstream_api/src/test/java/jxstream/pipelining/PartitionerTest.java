/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import jxstream.api.JxPipe;
import jxstream.api.JxPipe;
import jxstream.util.JxStreamUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author brimzi
 */
public class PartitionerTest {

    private JxPipe<Integer> ints;

    public PartitionerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
        ints = new JxPipe<>(Stream.iterate(1, x -> x + 1).limit(20));
    }
    
        /**
     * Test of getPartitions method, of class Partitioner.
     *
     * @throws java.io.IOException
     */
    @Test
    public void testGetPartitions() throws IllegalArgumentException, IOException {

        System.out.println("getPartitions");
        int parts = 4;
		HashedFlow<Integer> control = new HashedFlow(parts, x->x);
        Partitioner partitioner = new Partitioner(ints, control);
        List<JxPipe> result = partitioner.getPartitions();
        assertNotNull(result);
        assertEquals(parts, result.size());

        parts = 6;
		control = new HashedFlow(parts, x->x);
        partitioner = new Partitioner(ints, control);
        result = partitioner.getPartitions();
        assertNotNull(result);
        assertEquals(parts, result.size());

        partitioner.shutDown();
    }
    
    
    
    /**
     * Test of startAndGetUpstreamSwitch method, of class Partitioner.
     * @throws java.io.IOException
     */
    @Test
    public void testStartAndGetUpstreamSwitch() throws IOException {
        System.out.println("startAndGetUpstreamSwitch");
        int parts = 4;
		HashedFlow<Integer> control = new HashedFlow(parts, x->x);
        Partitioner partitioner = new Partitioner(ints, control);
        Runnable result = partitioner.getUpstreamSwitch();
        assertNotNull(result);
        
        partitioner.shutDown();
    }


    
    

    /**
     * Test of partitionStream method, of class Partitioner.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testPartitionStream() throws Exception {
        System.out.println("partitionStream");
		int parts = 2;
        JxPipe<Integer> expStream0 = new JxPipe<>(Stream.iterate(2, x -> x + 2).limit(10));
        JxPipe<Integer> expStream1 = new JxPipe<>(Stream.iterate(1, x -> x + 2).limit(10));

		HashedFlow<Integer> control = new HashedFlow(parts, x->x);
        Partitioner partitioner = new Partitioner(ints, control);
        final List<JxPipe<Integer>> results = partitioner.getPartitions();

        Runnable upstreamSwitch = partitioner.getUpstreamSwitch();
        assertNotNull(upstreamSwitch);

        upstreamSwitch.run();
        assertTrue("Streams don't match!", JxStreamUtil.compare(expStream0, results.get(0).limit(10)));
        assertTrue("Streams don't match!", JxStreamUtil.compare(expStream1, results.get(1).limit(10)));
        partitioner.shutDown();
		
		}


    /**
     * Test of shutDown method, of class Partitioner.
     */
    //@Test
    public void testShutDown() {
        System.out.println("shutDown");
        Partitioner instance = null;
        instance.shutDown();
        
    }

}
