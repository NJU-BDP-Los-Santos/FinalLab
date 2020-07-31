package task5;


import labelProgagation.labelPropagation;
import labelPropagation_iter.labelPropagation_iter;

public class Task5Main {
    public static void main(String[] args) throws Exception
    {
        try {
            //Configuration conf = new Configuration();
            //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            //if (otherArgs.length != 2) {
            //    System.err.println("Usage: <in> <out>");
            //    System.exit(2);
            //}
            String tmpDir = new String(args[1] + "tmp/iter_");
            int iterNum = 1;
            labelPropagation iter_1 = new labelPropagation();

            String tmp = new String(tmpDir + String.valueOf(iterNum));
            labelPropagation.labelpropagation(args[0], tmp);
            // args[0] is input path

            iterNum += 1;

            for(int i = 0; i< 4; i++)
            {
                String tmp2 = new String(tmpDir + String.valueOf(iterNum));
                labelPropagation_iter.labelpropagationiter(tmp, tmp2);
                tmp = tmp2;
                iterNum += 1;
            }

            labelPropagation_iter.labelpropagationiter(tmp, args[1] + "Final/");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void task5main(String inputPath, String outputPath, String tmpPath) throws Exception
    {
        try {
            //Configuration conf = new Configuration();
            //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            //if (otherArgs.length != 2) {
            //    System.err.println("Usage: <in> <out>");
            //    System.exit(2);
            //}
            String tmpDir = new String(tmpPath);
            int iterNum = 1;
            labelPropagation iter_1 = new labelPropagation();

            String tmp = new String(tmpDir + String.valueOf(iterNum));
            labelPropagation.labelpropagation(inputPath, tmp);

            iterNum += 1;

            for(int i = 0; i< 4; i++)
            {
                String tmp2 = new String(tmpDir + String.valueOf(iterNum));
                labelPropagation_iter.labelpropagationiter(tmp, tmp2);
                tmp = tmp2;
                iterNum += 1;
            }

            labelPropagation_iter.labelpropagationiter(tmp, outputPath);

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
