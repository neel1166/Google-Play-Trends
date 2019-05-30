import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GooglePlay {

  public static class Genere_Type_Mapper extends Mapper<Object, Text, Text, IntWritable>{

    private static final String COMMA_DELIMITER = ",";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] tokens = valueString.split(COMMA_DELIMITER);
    String result = String.join("-->", tokens[9].split(";")[0],tokens[6]);//mapping Genres to Type(paid or free)
    context.write(new Text(result.trim()), new IntWritable(1));
	}
}

  public static class Genere_Type_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	   private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if(result.get() > 1)
      {context.write(key, result);}
    }
}

public static class Genere_Ratings_Mapper extends Mapper<Object, Text, Text, IntWritable>{

  private static final String COMMA_DELIMITER = ",";
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  String valueString = value.toString();
  String[] tokens = valueString.split(COMMA_DELIMITER);
  String result = String.join("-->", tokens[9].split(";")[0],tokens[2]);//mapping Genres to Ratings
  context.write(new Text(result.trim()), new IntWritable(1));
}
}

public static class Genere_Ratings_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    if(result.get() > 1)
    {context.write(key, result);}

  }
}

public static class Genere_Installs_Mapper extends Mapper<Object, Text, Text, LongWritable>{

  private static final String COMMA_DELIMITER = ",";
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  String valueString = value.toString();
  String[] tokens = valueString.split(COMMA_DELIMITER);
  String result = tokens[9].split(";")[0];// Genere daata
  Long no_of_installs= Long.parseLong((tokens[5].replaceAll("[^0-9]", "0")).trim())/10;//Cleaning Installs data
  context.write(new Text(result.trim()), new LongWritable(no_of_installs));
}
}

public static class Genere_Installs_Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {


  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    Long sum = 0L;
    for (LongWritable val : values) {
        sum += val.get();
          }
    if(sum > 1){
      context.write(key, new LongWritable(sum));
    }
    }
  }

  public static class Count_Genres_Mapper extends Mapper<Object, Text, Text, IntWritable>{

    private static final String COMMA_DELIMITER = ",";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String valueString = value.toString();
		String[] tokens = valueString.split(COMMA_DELIMITER);
    String result = tokens[9].split(";")[0];// Genres Data
    context.write(new Text(result.trim()), new IntWritable(1));
	}
}

  public static class Count_Genres_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	   private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if(result.get() > 1)
      {context.write(key, result);}
    }
}

public static class App_Type_Mapper extends Mapper<Object, Text, Text, IntWritable>{

  private static final String COMMA_DELIMITER = ",";
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  String valueString = value.toString();
  String[] tokens = valueString.split(COMMA_DELIMITER);
  String result = tokens[6];// Genres Data
  context.write(new Text(result.trim()), new IntWritable(1));
}
}

public static class App_Type_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

 private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    if(result.get() > 1)
    {context.write(key, result);}
  }
}


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Mapping Genres and type");
    job.setJarByClass(GooglePlay.class);
    job.setMapperClass(Genere_Type_Mapper.class);
    job.setCombinerClass(Genere_Type_Reducer.class);
    job.setReducerClass(Genere_Type_Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Mapping Genres and Rating");
    job2.setJarByClass(GooglePlay.class);
    job2.setMapperClass(Genere_Ratings_Mapper.class);
    job2.setCombinerClass(Genere_Ratings_Reducer.class);
    job2.setReducerClass(Genere_Ratings_Reducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    job2.waitForCompletion(true);

    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "Mapping Genre and No. of Installs");
    job3.setJarByClass(GooglePlay.class);
    job3.setMapperClass(Genere_Installs_Mapper.class);
    job3.setCombinerClass(Genere_Installs_Reducer.class);
    job3.setReducerClass(Genere_Installs_Reducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job3, new Path(args[0]));
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    //System.exit(job3.waitForCompletion(true) ? 0 : 1);
    job3.waitForCompletion(true);

    Configuration conf4 = new Configuration();
    Job job4 = Job.getInstance(conf4, "Counting Generes");
    job4.setJarByClass(GooglePlay.class);
    job4.setMapperClass(Count_Genres_Mapper.class);
    job4.setCombinerClass(Count_Genres_Reducer.class);
    job4.setReducerClass(Count_Genres_Reducer.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job4, new Path(args[0]));
    FileOutputFormat.setOutputPath(job4, new Path(args[4]));
    //System.exit(job4.waitForCompletion(true) ? 0 : 1);
    job4.waitForCompletion(true);

    Configuration conf5 = new Configuration();
    Job job5 = Job.getInstance(conf5, "Counting types of App (Free or Paid)");
    System.out.println("Mapping Genre and type ");
    job5.setJarByClass(GooglePlay.class);
    job5.setMapperClass(App_Type_Mapper.class);
    job5.setCombinerClass(App_Type_Reducer.class);
    job5.setReducerClass(App_Type_Reducer.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job5, new Path(args[0]));
    FileOutputFormat.setOutputPath(job5, new Path(args[5]));
    System.exit(job5.waitForCompletion(true) ? 0 : 1);
    job3.waitForCompletion(true);

  }
}
