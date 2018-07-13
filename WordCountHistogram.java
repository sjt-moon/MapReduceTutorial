import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.Arrays;
import java.util.stream.StreamSupport;
import java.lang.Thread;
import java.lang.Class;
import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

/* Calculate word frequency histogram
* # work flow
* (word, 1) -> (word, count) -> (count, 1) -> (count, time of count)
*
* # TO BE SOLVED
* - can i use single WordCounter to count the 2nd and 3rd step?
*/

public class WordCountHistogram {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // if output directory exists, delete it
        Path outputPath = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        // generate intermediate path
        int k = -1;
        for (int i = args[1].length() - 1; i >= 0; i--) {
            if (args[1].charAt(i) == '\\') {
                k = i;
                break;
            }
        }
        Path intermediatePath;
        if (k > -1) intermediatePath = new Path(args[1].substring(k) + "/intermediate");
        else intermediatePath = new Path("./intermediate");
        if (hdfs.exists(intermediatePath)) {
            hdfs.delete(intermediatePath, true);
        }

        // first job: original text -> (word, frequency)
        Builder b1 = new Builder(conf, "word count");
        Job job1 = b1.getJob();
        b1.setJarByClass(WordCountHistogram.class).setMapperClass(WordTokenizer.class).setCombinerClass(WordCounter.class)
            .setReducerClass(WordCounter.class).setOutputKeyClass(Text.class).setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, intermediatePath);
        job1.waitForCompletion(true);

        // second job: (word, frequency) -> (frequency, word) -> (frequency, how many times we have words with this frequency)
        Builder b2 = new Builder(conf, "histogram counter");
        Job job2 = b2.getJob();
        b2.setJarByClass(WordCountHistogram.class).setMapperClass(ReverseMapper.class).setCombinerClass(IntCounter.class)
            .setReducerClass(IntCounter.class).setInputFormatClass(KeyValueTextInputFormat.class)
            .setOutputKeyClass(IntWritable.class).setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);

        // delete intermediate files
        if (hdfs.exists(intermediatePath)) {
            hdfs.delete(intermediatePath, true);
        }
    }

    /* Job Builder */
    public static class Builder {
        Job job;
        Builder(Configuration conf, String name) throws IOException {
            job = Job.getInstance(conf, name);
        }

        Builder setJarByClass(Class c) {
            job.setJarByClass(c);
            return this;
        }

        Builder setCombinerClass(Class c) {
            job.setCombinerClass(c);
            return this;
        }

        Builder setMapperClass(Class c) {
            job.setMapperClass(c);
            return this;
        }

        Builder setReducerClass(Class c) {
            job.setReducerClass(c);
            return this;
        }

        Builder setOutputKeyClass(Class c) {
            job.setOutputKeyClass(c);
            return this;
        }

        Builder setOutputValueClass(Class c) {
            job.setOutputValueClass(c);
            return this;
        }

        Builder setInputFormatClass(Class c) {
            job.setInputFormatClass(c);
            return this;
        }

        Builder setOutputFormatClass(Class c) {
            job.setOutputFormatClass(c);
            return this;
        }

        Job getJob() {
            return job;
        }
    }

    /* Map: original text -> (word, 1) */
    public static class WordTokenizer extends Mapper<Object, Text, Text, IntWritable> {
        final IntWritable uno = new IntWritable(1);
        final Text texto = new Text();

        @Override
        public void map(Object key, Text value, Context context) {
            String[] tokens = value.toString().split("[^a-zA-Z0-9]");
            Arrays.stream(tokens).forEach(token -> {
                texto.set(token.toLowerCase());
                try {
                    context.write(texto, uno);
                }
                catch (IOException | InterruptedException e) {
                    System.out.println("Error ocurrs @ WordTokenizer, yet I do not wanna do anything :D");
                }
            });
        }
    }

    /* Reduce: (word, 1) -> (word, word frequency) */
    public static class WordCounter extends Reducer<Text, IntWritable, Text, IntWritable> {
        final IntWritable cnt = new IntWritable(0);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = StreamSupport.stream(values.spliterator(), false).map(x -> x.get()).reduce(0, (x, y) -> x + y);
            cnt.set(sum);
            context.write(key, cnt);
        }
    }

    /* Mapper: reverse key-value pair
    * (word, frequency) -> (frequency, word)
    */
    static class ReverseMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
        @Override
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    /* Reducer: count word-frequency histogram
    * (frequency, word) -> (frequency, # of times this frequency occurs)
    */
    static class IntCounter extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        final IntWritable cnt = new IntWritable(0);

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) {
            int sum = StreamSupport.stream(values.spliterator(), false).map(x -> 1).reduce(0, (x, y) -> x + y);
            try {
                cnt.set(sum);
                context.write(key, cnt);
            }
            catch (IOException | InterruptedException e) {
                System.out.println("Error ocurrs @ IntCounter, yet I do not wanna do anything :D");
            }
        }
    }
}
