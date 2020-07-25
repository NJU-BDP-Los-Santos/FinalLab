package Task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task4PageRankViewer {
    public static class Task4Mapper extends Mapper<LongWritable, Text, FloatWritable, Text>
    {
        /*
         * value: Person PageRank@name1:r1;name2:r2;name3:r3; ...
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] strs = value.toString().split("@");
            String[] nameAndRank = strs[1].split("\\s+");
            String personName = nameAndRank[0];
            float rank = Float.parseFloat(nameAndRank[1]);
            context.write(new FloatWritable(rank), new Text(personName));
        }
    }

    public static class Task4Reducer extends Reducer<FloatWritable, Text, Text, Text>
    {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            float pageRank = key.get();
            for(Text each : values)
            {
                String personName = each.toString();
                context.write(new Text(personName), new Text(String.format("%.5f",pageRank)));
            }
        }
    }
}
