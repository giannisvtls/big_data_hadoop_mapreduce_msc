package Exercise1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NumeronymCount {

    public static class NumeronymMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text numeronym = new Text();

        public void map(Object key, Text value, Context context) {
            // Log unexpected errors without failing the job
            try {
                StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken();
                    if (word.length() >= 3) {
                        String numeronymStr = word.charAt(0) + String.valueOf(word.length() - 2) + word.charAt(word.length() - 1);
                        numeronym.set(numeronymStr);
                        context.write(numeronym, one);
                    }
                }
            } catch (Exception e) {
                System.err.println("Unexpected error on value: " + value + " - " + e.getMessage());
            }
        }
    }

    public static class NumeronymReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int minCount;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            minCount = conf.getInt("numeronym.min.count", 1);
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;

            // Log errors without failing the job
            try {
                for (IntWritable val : values) {
                    sum += val.get();
                }

                if (sum >= minCount) {
                    result.set(sum);
                    context.write(key, result);
                }
            } catch (Exception e) {
                System.err.println("Error processing key: " + key + " - " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("No value for min k appearances provided, defaulting to 10");
        }

        Integer k;
        try {
            k = (args.length > 0) ? Integer.parseInt(args[0]) : 10;
        } catch (NumberFormatException e) {
            System.err.println("Argument provided is not valid. Using default value of 10");
            k = 10;
        }

        Configuration conf = new Configuration();
        conf.setInt("numeronym.min.count", k);

        Path inputPath = new Path("src/main/scala/Exercise1/SherlockHolmes.txt");
        Path outputPath = new Path("src/main/scala/Exercise1/output");

        // Delete the output folder if it already exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "numeronym count");
        job.setJarByClass(NumeronymCount.class);
        job.setMapperClass(NumeronymMapper.class);
        job.setCombinerClass(NumeronymReducer.class);
        job.setReducerClass(NumeronymReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
