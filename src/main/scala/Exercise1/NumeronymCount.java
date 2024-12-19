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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (word.length() >= 3) {
                    String numeronymStr = word.charAt(0) + String.valueOf(word.length() - 2) + word.charAt(word.length() - 1);
                    numeronym.set(numeronymStr);
                    context.write(numeronym, one);
                }
            }
        }
    }

    public static class NumeronymReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int minCount;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            minCount = conf.getInt("numeronym.min.count", 1); // Default to 1 if not set
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= minCount) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NumeronymCount <input path> <output path> <min count>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("numeronym.min.count", Integer.parseInt(args[2]));

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); //delete the output folder if it already exists
        }

        Job job = Job.getInstance(conf, "numeronym count");
        job.setJarByClass(NumeronymCount.class);
        job.setMapperClass(NumeronymMapper.class);
        job.setCombinerClass(NumeronymReducer.class);
        job.setReducerClass(NumeronymReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("input"));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("output"));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
