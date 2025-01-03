package Exercise3;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class DNAMerCount {

    public static class MerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text mer = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int lineLength = line.length();

            // 2 counts

            for (int i = 0; i <= lineLength - 2; i++) {
                mer.set(line.substring(i, i + 2));
                context.write(mer, one);
            }

            // 3 counts
            for (int i = 0; i <= lineLength - 3; i++) {
                mer.set(line.substring(i, i + 3));
                context.write(mer, one);
            }

            // 4 counts
            for (int i = 0; i <= lineLength - 4; i++) {
                mer.set(line.substring(i, i + 4));
                context.write(mer, one);
            }
        }
    }

    public static class MerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path inputPath = new Path("src/main/scala/Exercise3/ecoli.txt");
        Path outputPath = new Path("src/main/scala/Exercise3/dna_output");

        // Delete the output folder if it already exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "DNA Mer Count");
        job.setJarByClass(DNAMerCount.class);
        job.setMapperClass(MerMapper.class);
        job.setCombinerClass(MerReducer.class);
        job.setReducerClass(MerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

