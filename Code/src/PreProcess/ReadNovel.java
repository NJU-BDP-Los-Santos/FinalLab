package PreProcess;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class ReadNovel
{
    public static class ReaderMapper extends Mapper<LongWritable, Text, Text, NullWritable>
    {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        /**
         * @description Mapper init. 需要建立一个用户自定义词典
         */
        {
            String nameFile = context.getConfiguration().get("nameFile");
            FileSystem fileSystem =FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new FileReader(nameFile));
            String nameline;
            while((nameline = br.readLine()) != null)
            // 不断读取文件，每一行是一个名字
            {
                DicLibrary.insert(DicLibrary.DEFAULT, nameline);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            Result result = DicAnalysis.parse(line);
            List<Term> terms = result.getTerms();
            StringBuilder sb = new StringBuilder();
            if (terms.size()>0)
            {
                for (int i = 0; i < terms.size(); i++)
                {
                    String word = terms.get(i).getName(); //拿到词
                    String natureStr = terms.get(i).getNatureStr(); //拿到词性
                    if (natureStr.equals("userDefine"))
                    // 用户定义的内容
                    {
                        sb.append(word + " ");
                    }
                }
            }
            String res = sb.length() > 0 ? sb.toString().substring(0,sb.length()-1):"";
            // 去除最后的空格
//            System.out.println(res);
            context.write(new Text(res),NullWritable.get());
        }
    }

    public static class ReaderReducer extends Reducer<Text, NullWritable,Text,NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
        {
            for (NullWritable value : values)
            {
                context.write(key,NullWritable.get());
            }
//            context.write(key,NullWritable.get());
        }
    }
}
