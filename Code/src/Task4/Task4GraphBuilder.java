package Task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task4GraphBuilder {
    public static class Task4Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        /*
         * value: Person name1:r1;name2:r2;name3:r3; ...
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            if(!line.contains(";")) return;
            String[] strs = line.split(" ");
            String name = strs[0];
            //initialized page rank
            String lists = "1@"+strs[1];
            context.write(new Text(name+" "+lists), new Text("1"));
        }
    }

    public static class Task4Reducer extends Reducer<Text, Text, Text, NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            context.write(new Text(key), NullWritable.get());
        }
    }
}
