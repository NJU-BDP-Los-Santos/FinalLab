package Task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Task3 {
    public static class Task3Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        /*
        * value: <name1,name2>times
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException
        {
            String interactionTimes = value.toString();
            if(!interactionTimes.contains("<")) return;
            Pattern pattern = Pattern.compile("(?<=<)[^>]+");
            Matcher matcher = pattern.matcher(interactionTimes);
            String[] nameTimes = interactionTimes.split(">");
            String[] names = null;
            while(matcher.find())
            {
                names = matcher.group().split(",");
            }
            Text newKey = new Text(names[0]);
            Text newValue = new Text(names[1]+","+nameTimes[1]);
            context.write(newKey, newValue);
            //Symmetrical
            Text symmetricalKey = new Text(names[1]);
            Text symmetricalValue = new Text(names[0]+","+nameTimes[1]);
            context.write(symmetricalKey, symmetricalValue);
        }
    }

    public static class Task3Reducer extends Reducer<Text, Text, Text, NullWritable>
    {
        /*
        * @param key firstPerson
        * @param values name1,t1 name2,t2 name3,t3 ...
        * @param context
        */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws  IOException, InterruptedException
        {
            double all = 0;
            StringBuilder sb = new StringBuilder();
            List<String> list = new ArrayList<>();
            for(Text each : values)
            {
                String line = each.toString();
                String[] value = line.split(",");
                list.add(line);
                all = all + Integer.parseInt(value[1]);
            }

            sb.append(key.toString()+" ");
            for(String each : list)
            {
                String[] value = each.split(",");
                double t = Integer.parseInt(value[1]);
                double ratio = t / all;
                sb.append(value[0]+":"+String.valueOf(ratio)+";");
            }
            //person name1:r1;name2:r2;name3:r3;...
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }
}
