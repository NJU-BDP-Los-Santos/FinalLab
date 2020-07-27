package labelProgagation;

import com.google.inject.internal.cglib.proxy.$Callback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.StringTokenizer;

public class labelPropagation {
    public static void main(String[] args) throws Exception
    {
        try {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: <in> <out>");
                System.exit(2);
            }

            Job job = new Job(conf, "Label-Propagation");
            job.setJarByClass(labelPropagation.class);
            job.setMapperClass(LabelMapper.class);
            job.setReducerClass(LabelReducer.class);
            job.setNumReduceTasks(4);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static class LabelMapper extends Mapper<LongWritable, Text,Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String name = line.split(" ")[0];
            String nameList = line.split(" ")[1];
            String label = new String(name);
            //System.out.println(label);
            StringTokenizer tokenizer = new StringTokenizer(nameList,";");
            while(tokenizer.hasMoreTokens()){
                String[] element = tokenizer.nextToken().split(":");
                context.write(new Text(element[0]),new Text(label+"#"+name));
            }
            context.write(new Text(name),new Text("#"+nameList));
            context.write(new Text(name),new Text("$"+label));
        }
    }

    public static class LabelReducer extends Reducer<Text, Text,Text, Text> {
        Map<String, String> name_label_map = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nameList = "";
            String label = "";
            Map<String,String> relation_name_label = new HashMap<>();

            for(Text text:values){
                String str = text.toString();
                if (str.length() > 0 && str.charAt(0) == '$'){
                    label = str.replace("$","");
                }
                else if (str.length() > 0 &&str.charAt(0) == '#'){
                    nameList = str.replace("#","");
                }
                else if (str.length() > 0){
                    String[] element = str.split("#");
                    relation_name_label.put(element[1],element[0]);
                }
            }

            Map<String,Float> label_pr_map = new HashMap<>();
            StringTokenizer nameList_Tokenizer = new StringTokenizer(nameList,";");
            while(nameList_Tokenizer.hasMoreTokens()){
                String[] name_pr = nameList_Tokenizer.nextToken().split(":");
                Float current_pr = Float.parseFloat(name_pr[1]);
                String current_label = relation_name_label.get(name_pr[0]);
                Float label_pr;
                if ((label_pr = label_pr_map.get(current_label)) != null){
                    label_pr_map.put(current_label,label_pr+current_pr);
                }else{
                    label_pr_map.put(current_label,current_pr);
                }
            }


            StringTokenizer tokenizer = new StringTokenizer(nameList,";");
            float maxPr = Float.MIN_VALUE;
            List<String> maxNameList = new ArrayList<>();
            while (tokenizer.hasMoreTokens()){
                String[] element = tokenizer.nextToken().split(":");
                float tmpPr = label_pr_map.get(relation_name_label.get(element[0]));
                if (maxPr < tmpPr){
                    maxNameList.clear();
                    maxPr = tmpPr;
                    maxNameList.add(element[0]);
                }else if (maxPr == tmpPr){
                    maxNameList.add(element[0]);
                }
            }
            Random random = new Random();
            int index = random.nextInt(maxNameList.size());
            String target_name = maxNameList.get(index);
            String target_label = relation_name_label.get(target_name);
            if (name_label_map.get(target_name) != null){
                target_label = name_label_map.get(target_name);
            }else{
                name_label_map.put(key.toString(),target_label);
            }
            if (target_label == null){
                System.out.println();
            }
            context.write(new Text(target_label + "#" + key.toString()),new Text(nameList));
        }

    }

}
