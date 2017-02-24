package hadooptest;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    
    public static class WordCounter implements WritableComparable<WordCounter> {
        private int counter;
        private String word;
        
        public static WordCounter create(String word, int counter) {
            WordCounter wc = new WordCounter();
            wc.word = word;
            wc.counter = counter;
            return wc;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            word = in.readUTF();
            counter = in.readInt();
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeInt(counter);
        }
        
        @Override
        public int compareTo(WordCounter o) {
            if (o.counter != counter) {
                return Integer.compare(o.counter, counter);
            } else {
                return word.compareTo(o.word);
            }
        }
        
        @Override
        public String toString() {
            return word;
        }
    }
    
    public static class TokenizerMapper
             extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context
                                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
                token = token.replaceFirst("^[^a-z\\-]*([a-z]*)[^a-z\\-]*$", "$1");
                if (token.matches("[a-z]+")) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer
             extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                                             Context context
                                             ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    
    public static class SortingMapper
        extends Mapper<Object, Text, WordCounter, IntWritable> {
        
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");
            int count = Integer.parseInt(parts[1]);
            context.write(WordCounter.create(parts[0], count), new IntWritable(count));
        }
    }
    
    public static class DummyReducer
            extends Reducer<WordCounter, IntWritable, WordCounter, IntWritable> {
        
    }
    
    public static void main(String[] args) throws Exception {
        String tempName = "temp" + System.currentTimeMillis();
        
        Job job = Job.getInstance(new Configuration(), "word count");
        job.setJarByClass(App.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tempName));
        job.waitForCompletion(true);
        
        Job job2 = Job.getInstance(new Configuration(), "result sort");
        job2.setJarByClass(App.class);
        job2.setMapperClass(SortingMapper.class);
        job2.setOutputKeyClass(WordCounter.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(tempName));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
        
    }
}

