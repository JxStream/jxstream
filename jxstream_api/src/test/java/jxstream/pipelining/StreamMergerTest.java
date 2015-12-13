/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import jxstream.api.JxPipe;
import jxstream.api.JxPipe;
import jxstream.util.JxStreamUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author brimzi
 */
public class StreamMergerTest {

    public StreamMergerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void testStreamCount() throws IOException {
        System.out.println("streamCount");
        List<JxPipe<Integer>> streams = new ArrayList<>();
        streams.add(new JxPipe(Stream.iterate(1, x -> x + 1).limit(20)));
        streams.add(new JxPipe(Stream.iterate(1, x -> x + 2).limit(20)));
        streams.add(new JxPipe(Stream.iterate(1, x -> x + 3).limit(20)));
        streams.add(new JxPipe(Stream.iterate(1, x -> x + 4).limit(20)));

        StreamMerger<Integer> merged = new StreamMerger<>(streams);

        assertEquals(merged.streamCount(), 4);

        List<JxPipe<Integer>> streams2 = new ArrayList<>();
        streams2.add(new JxPipe(Stream.iterate(1, x -> x + 1).limit(15)));

        streams2.add(new JxPipe(Stream.iterate(1, x -> x + 4).limit(15)));

        StreamMerger<Integer> merged2 = new StreamMerger<>(streams2);
        assertEquals(merged2.streamCount(), 2);

    }

    @Test
    public void testGetMergedStream() throws IOException {
        System.out.println("getMergedStream");
        List<JxPipe<Integer>> streams = new ArrayList<>();
        streams.add(new JxPipe(Stream.iterate(1, x -> x + 3).limit(7)));
        streams.add(new JxPipe(Stream.iterate(2, x -> x + 3).limit(7)));
        streams.add(new JxPipe(Stream.iterate(3, x -> x + 3).limit(6)));
        JxPipe<Integer> expected = new JxPipe<>(Stream.iterate(1, x -> x + 1).limit(20));

        final Executor threadpool=Executors.newCachedThreadPool();
        StreamMerger<Integer> merged = new StreamMerger<>(streams);
        JxPipe<Integer> result = merged.getMergedStream().limit(20);
        merged.getSwitches().stream().forEach(s->{
            threadpool.execute(s);
        });

        assertTrue("Streams do not match", JxStreamUtil.compareIgnoreOrder(expected, result));

    }


   

    
    
}
       
        
    



       
    
