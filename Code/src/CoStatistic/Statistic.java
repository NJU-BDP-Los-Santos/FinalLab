package CoStatistic;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Statistic
{
    public static class StatisticMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer tokens = new StringTokenizer(value.toString());

            Set<String> names = new HashSet<>();
            int token_count = 0;
            int times = 0;
            boolean is_times = false;

            while(tokens.hasMoreTokens())
            {
                token_count += 1;
//                word.set(tokens.nextToken());
//                Text word_filename = new Text(word + "#" + docName); // 创建复合键
//                context.write(word_filename, new IntWritable(1));
//                context.write(word, new Text(fileName));
                String token = tokens.nextToken();
                if (token.equals("TIMES:"))
                {
                    // 此时，下一个位置是频数
                    is_times = true;
                }
                else if (is_times)
                {
                    times = Integer.parseInt(token.toString());
                    is_times = false;
                }
                else
                {
                    names.add(token);
                }
            }

            if (token_count < 4)
            {
                // do nothing;
            }
            else
            {
                // 此时需要进行人名的排列组合
                for (String name1 : names)
                {
                    for (String name2 : names)
                    {
                        if (name1.compareTo(name2) < 0)
                        // 仅这种情况输出
                        {
                            String res = "<" + name1 + "," + name2 + ">";
                            context.write(new Text(res), new IntWritable(times));
                        }
                    }
                }
            }
        }
    }

    public static class StatisticCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException
        {
            int total = 0;
            for(IntWritable value: values)
            {
                total = total + value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static class StatisticReducer extends Reducer<Text, IntWritable,Text, NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable value : values)
            {
                count += value.get();
            }
            context.write(new Text(key.toString() + Integer.toString(count)), NullWritable.get());
        }
    }
}
