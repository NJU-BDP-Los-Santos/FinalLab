

import CoStatistic.CoStatisticMain;
import PreProcess.PreProcessMain;
import WjaMain.WjaMain;
import labelCluster.LabelClusterMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import rankPRvalue.RankPRMain;
import task5.Task5Main;

public class FinalMain
{
    public static void main(String[] args) throws Exception
    {
        /**
         * main函数传参说明
         * 0：input path
         * 1：output path
         */

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String input_path = otherArgs[0];
        String output_path = otherArgs[1];

        PreProcessMain.main(new String[]{input_path, output_path + "Task1/"});

        CoStatisticMain.main(new String[]{output_path + "Task1/", output_path + "Task2/"});

        WjaMain.main(new String[]{output_path + "Task2/", output_path + "Task3/", output_path + "Task4/"});

        Task5Main.task5main(output_path + "Task3/", output_path + "Task5/Final/", output_path + "Task5/Tmp/");

        LabelClusterMain.main(new String[]{output_path + "Task5/Final/", output_path + "Task6/LabelCluster/"});

        RankPRMain.main(new String[]{output_path + "Task4/Final/", output_path + "Task6/RankPR/"});

        System.exit(0);
    }
}
