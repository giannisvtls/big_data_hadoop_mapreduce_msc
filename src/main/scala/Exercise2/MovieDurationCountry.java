package Exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class MovieDurationCountry {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text country = new Text();
        private final LongWritable duration = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            // Skip the header of the csv file
            if (value.toString().startsWith("imdbID")) return;

            // Split rows and make an array
            String[] unfilteredFields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            // Some columns are missing resulting an empty string for that column, we can filter those out
            String[] fields = Arrays.stream(unfilteredFields)
                    .filter(field -> !field.equals(""))
                    .toArray(String[]::new);

            // Ignore fields with fewer columns
            if (fields.length < 9) return;

            // Log errors and continue the job
            try {
                // Get runtime
                String runtimeStr = fields[3].trim();

                // Runtime has the format "10 mins", we need to extract the numeric value
                runtimeStr = runtimeStr.replaceAll("[^0-9]", "");
                long movieDuration = Long.parseLong(runtimeStr);

                // Get countries, split entries with multiple countries
                String[] countries = fields[8].split(",");

                // Get duration for each country
                for (String c : countries) {
                    country.set(c.trim());
                    duration.set(movieDuration);
                    context.write(country, duration);
                }
            } catch (Exception e) {
                System.err.println("Error processing movie: " + value + " With error: " + e.getMessage());
            }
        }
    }

    public static class DurationSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Duration by Country");

        job.setJarByClass(MovieDurationCountry.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(DurationSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path("src/main/scala/Exercise2/movies.csv"));
        FileSystem.get(conf).delete(new Path("src/main/scala/Exercise2/moviesDuration"),true);
        TextOutputFormat.setOutputPath(job, new Path("src/main/scala/Exercise2/moviesDuration"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}