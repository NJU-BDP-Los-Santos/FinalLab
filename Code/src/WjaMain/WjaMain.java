package WjaMain;

import Task3.Task3;
import Task4.Task4GraphBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WjaMain {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //----------------------Task3-----------------------//
        /*Job job3 = Job.getInstance();
        job3.setJarByClass(WjaMain.class);

        job3.setMapperClass(Task3.Task3Mapper.class);
        job3.setReducerClass(Task3.Task3Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(NullWritable.class);

        //Specify your own path for task3
        FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));

        job3.waitForCompletion(true);*/

        //---------------------Task4------------------------//
        Job job4GraphBuilder = Job.getInstance();
        job4GraphBuilder.setJarByClass(WjaMain.class);
        job4GraphBuilder.setNumReduceTasks(2);

        job4GraphBuilder.setMapperClass(Task4GraphBuilder.Task4Mapper.class);
        job4GraphBuilder.setReducerClass(Task4GraphBuilder.Task4Reducer.class);

        job4GraphBuilder.setMapOutputKeyClass(Text.class);
        job4GraphBuilder.setMapOutputValueClass(Text.class);

        job4GraphBuilder.setOutputKeyClass(Text.class);
        job4GraphBuilder.setOutputValueClass(NullWritable.class);

        //Specify your own path for task4
        FileInputFormat.addInputPath(job4GraphBuilder, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job4GraphBuilder, new Path(otherArgs[1]));

        job4GraphBuilder.waitForCompletion(true);
    }
}
