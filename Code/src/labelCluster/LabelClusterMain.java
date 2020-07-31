package labelCluster;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class LabelClusterMain {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "Label-Cluster");
            job.setJarByClass(LabelClusterMain.class);
            job.setMapperClass(LabelMapper.class);
            job.setReducerClass(LabelReducer.class);
            job.setNumReduceTasks(1);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//            System.exit(job.waitForCompletion(true) ? 0 : 1);
            job.waitForCompletion(true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class LabelMapper extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String label_name = line.split("\t")[0];
            String nameList = line.split("\t")[1];
            //System.out.println(label_name);
            String name = label_name.split("#")[1];
            String label = label_name.split("#")[0];

            context.write(new Text(label),new Text(name));

        }
    }

    public static class LabelReducer extends Reducer<Text, Text,Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text text:values)
            {
                context.write(key, text);
            }
        }
    }
}
