package WjaMain;

import Task3.Task3;
import Task4.Task4GraphBuilder;
import Task4.Task4PageRankIter;
import Task4.Task4PageRankViewer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.thread.Timeout;

import java.io.File;
import java.io.IOException;

public class WjaMain {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

//        CleanTask3(otherArgs[1]);
//        CleanTask4(otherArgs[2]);

        //----------------------Task3-----------------------//
        Task3(otherArgs[0], otherArgs[1]);

        //---------------------Task4------------------------//
        Task4(otherArgs[1], otherArgs[2]);
    }

    private static void Task3(String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Job job3 = Job.getInstance();
        job3.setJarByClass(WjaMain.class);

        job3.setMapperClass(Task3.Task3Mapper.class);
        job3.setReducerClass(Task3.Task3Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(NullWritable.class);

        //Specify your own path for task3
        FileInputFormat.addInputPath(job3, new Path(inputPath));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));

        job3.waitForCompletion(true);
    }

    static final int IterationTimes = 30;
    private static void Task4(String inputPath, String outputPath)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Job job4GraphBuilder = Job.getInstance();
        job4GraphBuilder.setJarByClass(WjaMain.class);
        job4GraphBuilder.setNumReduceTasks(2);

        job4GraphBuilder.setMapperClass(Task4GraphBuilder.Task4Mapper.class);
        job4GraphBuilder.setReducerClass(Task4GraphBuilder.Task4Reducer.class);

        job4GraphBuilder.setMapOutputKeyClass(Text.class);
        job4GraphBuilder.setMapOutputValueClass(Text.class);

        job4GraphBuilder.setOutputKeyClass(Text.class);
        job4GraphBuilder.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job4GraphBuilder, new Path(inputPath));
        FileOutputFormat.setOutputPath(job4GraphBuilder, new Path(outputPath + "Trash/outputTask4Iter0/"));

        job4GraphBuilder.waitForCompletion(true);


        for(int i = 1; i <= IterationTimes; i++)
        {
            Job job4PageRankIter = Job.getInstance();
            job4PageRankIter.setJarByClass(WjaMain.class);
            job4PageRankIter.setNumReduceTasks(4);

            job4PageRankIter.setMapperClass(Task4PageRankIter.Task4Mapper.class);
            job4PageRankIter.setReducerClass(Task4PageRankIter.Task4Reducer.class);

            job4PageRankIter.setMapOutputKeyClass(Text.class);
            job4PageRankIter.setMapOutputValueClass(Text.class);

            job4PageRankIter.setOutputKeyClass(Text.class);
            job4PageRankIter.setOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job4PageRankIter, new Path(outputPath + "Trash/outputTask4Iter"+(i-1)+"/"));
            FileOutputFormat.setOutputPath(job4PageRankIter, new Path(outputPath + "Trash/outputTask4Iter"+i+"/"));

            job4PageRankIter.waitForCompletion(true);
        }


        Job job4PageRankViewer = Job.getInstance();
        job4PageRankViewer.setJarByClass(WjaMain.class);
        job4PageRankViewer.setNumReduceTasks(1);
        job4PageRankViewer.setSortComparatorClass(Task4PageRankViewer.FloatWritableDescendingComparator.class);

        job4PageRankViewer.setMapperClass(Task4PageRankViewer.Task4Mapper.class);
        job4PageRankViewer.setReducerClass(Task4PageRankViewer.Task4Reducer.class);

        job4PageRankViewer.setMapOutputKeyClass(FloatWritable.class);
        job4PageRankViewer.setMapOutputValueClass(Text.class);

        job4PageRankViewer.setOutputKeyClass(Text.class);
        job4PageRankViewer.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4PageRankViewer, new Path(outputPath + "Trash/outputTask4Iter"+IterationTimes+"/"));
        FileOutputFormat.setOutputPath(job4PageRankViewer, new Path(outputPath + "Final/"));

        job4PageRankViewer.waitForCompletion(true);
    }

    public static void CleanTask3(String outputPath)
    {
        DeleteFiles(outputPath);
    }

    public static void CleanTask4(String outputPath)
    {
        DeleteFiles(outputPath);
        for(int i = 0; i <= IterationTimes; i++)
        {
            DeleteFiles("outputTask4Iter"+i+"/");
        }
    }

    public static void DeleteFiles(String folderPath)
    {
        File file = new File(folderPath);
        if(file.isFile())
        {
            file.delete();
        }
        else
        {
            File[] files = file.listFiles();
            if(files == null)
            {
                file.delete();
            }
            else
            {
                for(int i = 0; i < files.length; i++)
                {
                    DeleteFiles(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }
}
