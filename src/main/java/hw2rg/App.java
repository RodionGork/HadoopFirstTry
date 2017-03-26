package hw2rg;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    
    public static class WordLengthMapper
             extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable dummyKey = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                word.set(token);
                context.write(dummyKey, word);
            }
        }
    }

    public static class WordLengthReducer
             extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String best = null;
            int bestLength = 0;
            for (Text w : values) {
                int len = w.getLength();
                if (len > bestLength) {
                    best = w.toString();
                    bestLength = len;
                }
            }
            context.write(key, new Text(best));
        }
    }
    
    
    private void run(String inputPath, String outputPath) throws Exception {
        Job job = Job.getInstance(new Configuration(), "word lengths");
        job.setJarByClass(App.class);
        job.setMapperClass(WordLengthMapper.class);
        job.setCombinerClass(WordLengthReducer.class);
        job.setReducerClass(WordLengthReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
    
    public static void main(String[] args) throws Exception {
        new App().run(args[0], args[1]);
    }
}

