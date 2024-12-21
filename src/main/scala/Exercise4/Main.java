package Exercise4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
  // First Job: Calculate vertex degrees
  public static class DegreeMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private double threshold;

    @Override
    protected void setup(Context context) {
      threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\\s+");
      int v1 = Integer.parseInt(parts[0]);
      int v2 = Integer.parseInt(parts[1]);
      double prob = Double.parseDouble(parts[2]);

      if (prob >= threshold) {
        context.write(new IntWritable(v1), new DoubleWritable(prob));
        context.write(new IntWritable(v2), new DoubleWritable(prob));
      }
    }
  }

  public static class DegreeReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private double totalSum = 0.0;
    private int count = 0;

    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
      double sum = 0.0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }

      totalSum += sum;
      count++;
      context.write(key, new DoubleWritable(sum));
    }

    @Override
    protected void cleanup(Context context) {
      // Use counters instead of configuration
      context.getCounter("Mean", "Sum").setValue((long) (totalSum * 1000));
      context.getCounter("Mean", "Count").setValue(count);
    }
  }

  // Second Job: Filter vertices above mean
  public static class AboveMeanMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private double meanDegree;

    @Override
    protected void setup(Context context) {
      meanDegree = Double.parseDouble(context.getConfiguration().get("mean_degree"));
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] parts = value.toString().split("\\s+");
      int vertex = Integer.parseInt(parts[0]);
      double degree = Double.parseDouble(parts[1]);

      if (degree > meanDegree) {
        context.write(new IntWritable(vertex), new DoubleWritable(degree));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("No threshold argument provided, defaulting to 0.5");
    }

    double threshold;
    try {
      threshold = (args.length > 0) ? Double.parseDouble(args[0]) : 0.5;
    } catch (NumberFormatException e) {
      System.err.println("Argument provided is not valid. Using default value of 0.5");
      threshold = 0.5;
    }

    // Threshold Validation
    if (threshold < 0 | threshold > 1) {
      System.err.println("Threshold not in the desired range of 0-1. Using default value of 0.5");
      threshold = 0.5;
    }

    String inputPath = "src/main/scala/Exercise4/collins.txt";
    String outputPath = "src/main/scala/Exercise4/output";

    String firstJobOutputPath = outputPath + "/task1";

    // Job 1: Calculate vertex degrees
    Configuration conf1 = new Configuration();
    conf1.set("threshold", String.valueOf(threshold));

    Job job1 = Job.getInstance(conf1, "Vertex Degree Calculation");
    job1.setJarByClass(Main.class);

    job1.setMapperClass(DegreeMapper.class);
    job1.setReducerClass(DegreeReducer.class);

    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job1, new Path(inputPath));
    FileSystem.get(conf1).delete(new Path(firstJobOutputPath),true);
    FileOutputFormat.setOutputPath(job1, new Path(firstJobOutputPath));

    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

    // Calculate mean from counters
    // Dividing with 1000 to get back a decimal
    double totalSum = job1.getCounters().findCounter("Mean", "Sum").getValue() / 1000.0;
    long count = job1.getCounters().findCounter("Mean", "Count").getValue();
    double meanDegree = totalSum / count;

    String secondJobOutputPath = outputPath + "/task2";
    Configuration conf2 = new Configuration();
    conf2.set("mean_degree", String.valueOf(meanDegree));

    Job job2 = Job.getInstance(conf2, "Above Mean Vertices");
    job2.setJarByClass(Main.class);

    job2.setMapperClass(AboveMeanMapper.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job2, new Path(inputPath));
    FileSystem.get(conf2).delete(new Path(secondJobOutputPath),true);
    FileOutputFormat.setOutputPath(job2, new Path(secondJobOutputPath));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}