package Exercise2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MovieGenreYearCount {

  public static class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text yearGenre = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Skip the header of the csv file
      if (value.toString().startsWith("imdbID")) return;

      String[] fields = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

      // Ignore fields with fewer columns
      if (fields.length < 9) return;

      try {
        // Get IMDb rating (field index 6)
        double score = Double.parseDouble(fields[6].trim());

        // Only process if score > 8
        if (score > 8.0) {
          String year = fields[2].trim();
          String[] genres = fields[4].replace("\"", "").split(",");

          // Emit count for each genre
          for (String genre : genres) {
            yearGenre.set(year + "_" + genre.trim());
            context.write(yearGenre, one);
          }
        }
      } catch (NumberFormatException e) {
        // Skip invalid entries
      }
    }
  }

  public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
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
    Job job = Job.getInstance(conf, "Movie Count by Year and Genre");

    job.setJarByClass(MovieGenreYearCount.class);
    job.setMapperClass(MovieMapper.class);
    job.setReducerClass(CountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path("src/main/scala/Exercise2/movies.csv"));
    FileSystem.get(conf).delete(new Path("src/main/scala/Exercise2/moviesGenreAndYear"),true);
    FileOutputFormat.setOutputPath(job, new Path("src/main/scala/Exercise2/moviesGenreAndYear"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}