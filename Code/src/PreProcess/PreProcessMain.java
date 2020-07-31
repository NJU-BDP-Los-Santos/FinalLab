package PreProcess;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PreProcessMain
        /**
         * @author GRP
         * @date 2020.7.25
         * @description PreProcess对应了 任务1 数据预处理，将金庸的小说分隔成为只有名字的小说
         */
{
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            conf.set("nameFile", otherArgs[0] + "People_List_unique.txt");
            if (otherArgs.length != 2)
            {
                System.err.println("Please Use the command: <input path> <output path>");
                System.exit(2);
            }

            Job job = new Job(conf, "PreProcess");
            job.setJarByClass(PreProcessMain.class);
            job.setMapperClass(ReadNovel.ReaderMapper.class);
            job.setCombinerClass(ReadNovel.ReaderCombiner.class);
            job.setReducerClass(ReadNovel.ReaderReducer.class);
//            job.setCombinerClass(NullPointerException.class);
//            job.setCombinerClass(CombinerSameWordDoc.class);
//            job.setPartitionerClass(PidPartitioner.class);
            job.setNumReduceTasks(2);
//            job.setGroupingComparatorClass(XGroup.class);
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "wuxia_novels/"));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//            System.exit(job.waitForCompletion(true) ? 0 : 1);
            job.waitForCompletion(true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
