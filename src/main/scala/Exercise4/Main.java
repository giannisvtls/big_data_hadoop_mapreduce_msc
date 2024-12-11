package Exercise4;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import static org.apache.spark.sql.functions.*;

public class Main {
  // Edge class to represent graph edges
  public static class Edge {
    private int source;
    private int target;
    private double probability;

    public Edge(int source, int target, double probability) {
      this.source = source;
      this.target = target;
      this.probability = probability;
    }

    // Getters required for DataFrame conversion
    public int getSource() { return source; }
    public int getTarget() { return target; }
    public double getProbability() { return probability; }
  }

  public static void main(String[] args) {
    // Validate command line arguments
    if (args.length != 2) {
      System.out.println("Usage: GraphAnalysis <input_file> <threshold>");
      System.exit(1);
    }

    String inputFile = "src/main/scala/Exercise4/" + args[0];    // collins.txt
    double threshold = Double.parseDouble(args[1]);  // probability threshold T

    // Create Spark session
    SparkSession spark = SparkSession.builder()
      .appName("Graph Analysis")
      .config("spark.master", "local[*]")
      .getOrCreate();

    // Read the input file
    Dataset<Row> edgesDF = spark.read()
      .option("delimiter", " ")
      .csv(inputFile)
      .toDF("source", "target", "probability")
      .select(
        col("source").cast("int"),
        col("target").cast("int"),
        col("probability").cast("double")
      );

    // Filter edges based on threshold
    Dataset<Row> filteredEdges = edgesDF.filter(col("probability").geq(threshold));

    // Calculate average degree for each vertex
    Dataset<Row> degrees = filteredEdges
      .select(
        explode(array(col("source"), col("target"))).as("vertex"),
        col("probability")
      )
      .groupBy("vertex")
      .agg(sum("probability").as("avg_degree"));

    // Calculate global average degree
    double globalAvgDegree = degrees.agg(avg("avg_degree"))
      .first()
      .getDouble(0);

    // Task 1: Show average degree for all vertices
    System.out.println("Task 1: Average degrees (threshold = " + threshold + ")");
    Dataset<Row> task1Results = degrees
      .orderBy("vertex")
      .select(
        col("vertex"),
        functions.format_number(col("avg_degree"), 2).as("avg_degree")
      );

    // Save Task 1 results
    task1Results.write()
      .mode("overwrite")
      .option("header", "true")
      .csv("src/main/scala/Exercise4/output_task1");

    // Task 2: Show vertices with above-average degree
    System.out.printf("\nTask 2: Vertices with above average degree (global average = %.2f)\n",
      globalAvgDegree);

    Dataset<Row> task2Results = degrees
      .filter(col("avg_degree").gt(globalAvgDegree))
      .orderBy(col("avg_degree").desc())
      .select(
        col("vertex"),
        functions.format_number(col("avg_degree"), 2).as("avg_degree")
      );

    // Save Task 2 results
    task2Results.write()
      .mode("overwrite")
      .option("header", "true")
      .csv("src/main/scala/Exercise4/output_task2");

    // Display sample results in console
    System.out.println("\nSample results (top 20 vertices with highest degrees):");
    task2Results.show(20, false);

    spark.stop();
  }
}