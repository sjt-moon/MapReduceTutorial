import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.Arrays;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
        static final Random rand = new Random(895);
        static final LongWritable number = new LongWritable(0);
        static final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) {
            String[] tokens = value.toString().split("[^a-z0-9A-Z]");
            Arrays.stream(tokens).forEach(token -> {
                token = token.toLowerCase();
                word.set(token);
                number.set((long)rand.nextLong() % (long)10.0);
                try {
                    context.write(word, number);
                }
                catch (IOException | InterruptedException e) {

                }
            });
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        static final LongWritable result = new LongWritable(0);

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = StreamSupport.stream(values.spliterator(), false).map(v -> v.get()).reduce((long)0.0, (x, y) -> x + y);
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
