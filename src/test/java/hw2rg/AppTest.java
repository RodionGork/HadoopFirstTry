package hw2rg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class AppTest {

    private List<String> sampleInput = Arrays.asList("what god hath wrought is wonder".split("\\s"));

    private MapDriver<Object, Text, IntWritable, Text> mapDriver;
    private ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    private MapReduceDriver<Object, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void init() {
        App.WordLengthMapper mapper = new App.WordLengthMapper();
        App.WordLengthReducer reducer = new App.WordLengthReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text(String.join(" ", sampleInput)));
        for (String s : sampleInput) {
            mapDriver.withOutput(new IntWritable(1), new Text(s));
        }
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        String best = sampleInput.stream().max(Comparator.comparingInt(String::length)).get();
        reduceDriver.withInput(new IntWritable(1),
                sampleInput.stream().map(Text::new).collect(Collectors.toList()));
        reduceDriver.withOutput(new IntWritable(1), new Text(best));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        for (String s : new String[] {"stone walls do not", "a prison make", "nor iron bars a cage"}) {
            mapReduceDriver.withInput(new LongWritable(0), new Text(s));
        }
        mapReduceDriver.withOutput(new IntWritable(1), new Text("prison"));
        mapReduceDriver.runTest();
    }
}
