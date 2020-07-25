package Task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task4PageRankIter {
    public static class Task4Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        /*
         * value: Person rank@name1:r1;name2:r2;name3:r3; ...
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] strs = value.toString().split("\\s+");
            String personName = strs[0];
            String[] pageRanks = strs[1].split("@");
            String pageRank = pageRanks[0];
            String[] lists = pageRanks[1].split(";");
            Text textPersonName = new Text(personName);
            Text textPageRank = new Text(pageRank);
            context.write(textPersonName, new Text(pageRanks[1]));
            for(String each : lists)
            {
                String[] nameAndRatio = each.split(":");
                context.write(new Text(nameAndRatio[0]), textPageRank);
            }
        }
    }

    public static class Task4Reducer extends Reducer<Text, Text, Text, Text>
    {
        /*
        * @param key     : personName
        * @param values  : pageRank1,pageRank2,pageRank3...,outerLinks
        * @param context :
        */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String outerLinks = null;
            double pageRank = 0;
            for(Text each : values)
            {
                String line = each.toString();
                if (line.contains(";"))
                {
                    outerLinks = line;
                }
                else
                {
                    pageRank = pageRank + Double.parseDouble(line);
                }
            }
            context.write(key, new Text(String.format("%.5f",pageRank)+"@"+outerLinks));
        }
    }
}