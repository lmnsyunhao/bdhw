import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Solution for Big Data HomeWork 2 Part 1. 
 * Read Some dirty (has noise) triple. 
 * Accumulate the avgerage of value and count numbers.
 * 
 * @author yunhao
 */
public class Hw2Part1 {

  /** Store the input file argument. */
  private static String argInput;
  /** Store the output file argument. */
  private static String argOutput;

  /**
   * Mapper class. It's used to break the big problem into many trivial ones. 
   */
  public static class DispatchMapper extends Mapper<Object, Text, Text, Text> {

    /** Output Key for map function. */
    private Text outKey = new Text();
    /** Output Value for map function. */
    private Text outValue = new Text();

    /**
     * Map function in Mapper class.
     * 
     * @param key Original key from file.
     * @param value Original value from file.
     * @param context Pass the intermediate value to Reducer.
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] items = value.toString().trim().split("\\s+");
      if(items.length != 3) return;
      if(!items[2].matches("[0-9]+(\\.[0-9]+)?")) return;
      outKey.set(items[0] + " " + items[1]);
      outValue.set(items[2]);
      context.write(outKey, outValue);
    }

  }

  /**
   * Combiner class. Local Reducer. Merge the result in local value.
   */
  public static class CountAverageCombiner extends Reducer<Text, Text, Text, Text> {

    /** Store the temperory result. */
    private Text tempResult = new Text();

    /**
     * Reduce function in local Reducer class.
     * 
     * @param key Intermediate key from context in Mapper class map function.
     * @param values Intermediate value from context in Mapper class map function.
     * @param context Pass the intermediate value to Global Reducer.
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double sum = 0;
      int cnt = 0;
      for (Text val : values){
        sum += Double.parseDouble(val.toString());
        cnt++;
      }
      tempResult.set(cnt + " " + sum);
      context.write(key, tempResult);
    }

  }

  /**
   * Reducer class. Global Reducer. Get final result.
   */
  public static class CountAverageReducer extends Reducer<Text,Text,Text,Text> {

    /** Store the final result. */
    private Text result = new Text();

    /**
     * Reduce function in Global Reducer class.
     * 
     * @param key Intermediate key from context in Combiner class reduce function.
     * @param values Intermediate value from context in Combiner class reduce function.
     * @param context Pass the result out.
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      double sum = 0;
      int cnt = 0;
      for (Text val : values) {
        String[] items = val.toString().split(" ");
        cnt += Integer.parseInt(items[0]);
        sum += Double.parseDouble(items[1]);
      }
      sum /= Double.parseDouble((new Integer(cnt)).toString());
      result.set(cnt + " " + (new DecimalFormat("#.000")).format(sum));
      context.write(key, result);
    }

  }

  /** Main procedure of Hw2Part1 class. */
  public static void main(String[] args) throws Exception {
    // Parsing the arguments from command line.
    String[] arguments = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
    if (arguments.length < 2) {
      System.err.println("Arguments absent");
      System.exit(2);
    }
    argInput = arguments[0];
    argOutput = arguments[1];

    // Setting the Map-Reduce job.
    Job job = Job.getInstance(new Configuration(), "Count And Average");
    job.setJarByClass(Hw2Part1.class);
    job.setMapperClass(DispatchMapper.class);
    job.setCombinerClass(CountAverageCombiner.class);
    job.setReducerClass(CountAverageReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Adding input file and output file path.
    for (int i = 0; i < arguments.length - 1; i++)
      FileInputFormat.addInputPath(job, new Path(arguments[i]));
    FileOutputFormat.setOutputPath(job, new Path(arguments[arguments.length - 1]));

    // Starting Map-Reduce job and wait from exit.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}
