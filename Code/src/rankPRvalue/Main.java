package rankPRvalue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "rank PRvalue");
            job.setJarByClass(Main.class);
            job.setSortComparatorClass(DescSort.class);
            job.setMapperClass(Main.rankMapper.class);
            job.setReducerClass(Main.rankReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(FloatWritable.class);
            job.setOutputValueClass(Text.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class rankMapper extends Mapper<Object,Text, FloatWritable,Text>{

        @Override
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String line=value.toString();
            String arr[]=line.split("\t");
            Text name = new Text(arr[0]);
            FloatWritable prValue = new FloatWritable(Float.parseFloat(arr[1]));
            context.write(prValue,name);
        }
    }

    public static class rankReducer extends Reducer<FloatWritable, Text, Text, FloatWritable>{

        @Override
        public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val:values){
                context.write(val, key);
            }
        }
    }

}
