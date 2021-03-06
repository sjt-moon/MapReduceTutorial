import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainMR extends Configured implements Tool{
    static Configuration cf;

    //implementing CHAIN MAPREDUCE without using custom format

    //SPLIT MAPPER
    public static class SplitMapper extends Mapper<Object,Text,Text,IntWritable> {
        private IntWritable uno = new IntWritable(1);
        //private String content;
        private String tokens[];
        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException {
            tokens=value.toString().split("[^a-zA-Z0-9]");
            for(String x:tokens) {
                context.write(new Text(x), uno);
            }
        }
    }


    //UPPER CASE MAPPER
    public static class UpperCaseMapper extends Mapper<Text,IntWritable,Text,IntWritable> {
        @Override
        public void map(Text key,IntWritable value,Context context)throws IOException,InterruptedException {
            String val=key.toString().toUpperCase();
            Text newKey=new Text(val);
            context.write(newKey, value);
        }
    }



    //ChainMapReducer
    public static class ChainMapReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private int sum=0;
        @Override
        public void reduce(Text key,Iterable<IntWritable>values,Context context)throws IOException,InterruptedException{
            for(IntWritable value:values)
            {
                sum+=value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public int run (String args[])throws IOException,InterruptedException,ClassNotFoundException{
        cf=new Configuration();

        //bypassing the GenericOptionsParser part and directly running into job declaration part
        Job j=Job.getInstance(cf);

        /**************CHAIN MAPPER AREA STARTS********************************/
        Configuration splitMapConfig=new Configuration(false);
        //below we add the 1st mapper class under ChainMapper Class
        ChainMapper.addMapper(j, SplitMapper.class, Object.class, Text.class, Text.class, IntWritable.class, splitMapConfig);

        //configuration for second mapper
        Configuration upperCaseConfig=new Configuration(false);
        //below we add the 2nd mapper that is the lower case mapper to the Chain Mapper class
        ChainMapper.addMapper(j, UpperCaseMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, upperCaseConfig);
        /**************CHAIN MAPPER AREA FINISHES********************************/

        //now proceeding with the normal delivery
        j.setJarByClass(ChainMR.class);
        j.setCombinerClass(ChainMapReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        Path p=new Path(args[1]);

        //set the input and output URI
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, p);
        p.getFileSystem(cf).delete(p, true);
        return j.waitForCompletion(true)?0:1;
    }

    public static void main(String args[])throws Exception{
        int res=ToolRunner.run(cf, new ChainMR(), args);
        System.exit(res);
    }
}
