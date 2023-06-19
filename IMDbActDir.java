import java.io.IOException;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;

public class IMDbActDir {

  public static class ActorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String record = value.toString();
      String[] parts = record.split(";");
      context.write(new Text(parts[0]), new Text(String.join(";", "Actor", parts[1], parts[2])));
    }
  }

  public static class DirectorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String record = value.toString();
      String[] parts = record.split(";");
      context.write(new Text(parts[0]), new Text(String.join(";", "Director", parts[1])));
    }
  }

  public static class TitlesMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String record = value.toString();
      String[] parts = record.split(";");
      String titleYear = parts[3].strip().equals("\\N") ? "UNKNWN_YR" : parts[3].strip();
      String titleGenre = parts[4].strip().equals("\\N") ? "UNKNWN_GNR" : parts[4].strip();
      context.write(new Text(parts[0]), new Text(String.join(";", "Title", parts[1], parts[2], titleYear, titleGenre)));
    }
  }

  public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
    long counter = 0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String titleType = "";
      String titleName = "";
      String titleGenre = "";
      String titleYear = "";
      int vCount = 0;

      HashMap<String, String> actorList = new HashMap<>();

      List<String> directorList = new ArrayList<String>();

      Boolean isComplete = false;

      Boolean actedAndDirected = false;
      String directorName = "";

      String outputStr = "";

      for (Text t : values) {
        String parts[] = t.toString().split(";");
        if (parts[0].equals("Title") && parts.length >= 5) {
          //outputStr = outputStr + " " + t.toString() + " " + "count(" + parts.length + ")"; // Output
          titleType = parts[1];
          titleName = parts[2];
          titleYear = parts[3];
          titleGenre = parts[4];
        } else if (parts[0].equals("Actor")) {
          // outputStr = outputStr + " " + parts[2];
          actorList.put(parts[1], parts[2]);
        } else if (parts[0].equals("Director")) {
          // outputStr = outputStr + " " + parts[1];
          directorList.add(parts[1]);
        } else {
          //outputStr = outputStr + t.toString() + "\t"; // output
        }
      }

      // director checker
      for (String id : directorList) {
        if (actorList.containsKey(id)) {
          directorName = actorList.get(id);
          actedAndDirected = true;
          break;
        }
      }

      if (actedAndDirected && 
          (titleGenre.contains("Romance") | 
          titleGenre.contains("Action") | 
          titleGenre.contains("Comedy")) &&
          ("tvMovie movie short".contains(titleType))) {
            //outputStr = outputStr + "===" + directorName + "=:\t"; // Output
            outputStr = titleType+"\t"+titleName+"\t"+titleYear+"\t"+titleGenre+"\t"+directorName;
            context.write(new Text(key), new Text(outputStr));          
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.min.split.size", "67108864");
    conf.set("mapreduce.map.memory.mb", "4096");
    conf.set("mapreduce.reduce.memory.mb", "4096");
    Job job1 = Job.getInstance(conf, "actor-director gig");
    job1.setJarByClass(IMDbActDir.class);
    job1.setNumReduceTasks(3);

    System.out.println(args[0] + "," + args[1]);

    MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, ActorsMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, DirectorsMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, TitlesMapper.class);

    job1.setReducerClass(JoinReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    FileOutputFormat.setOutputPath(job1, new Path(args[3])); // Here we are creating an intermediate folder to store 
    // output from first job
    System.exit(job1.waitForCompletion(true) ? 0 : 1);
  }
}
