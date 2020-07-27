package Task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
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
            String line = value.toString();
            if(!line.contains("@")) return;
            String[] strs = line.split("@");
            String[] nameAndRank = strs[0].split("\\s+");
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

    public static class FloatWritableDescendingComparator extends FloatWritable.Comparator
    {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}
