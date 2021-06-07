package employee;

// How many awards were obtained by the employee whose salary is 30000?

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Prog3 {

  //MAPPER CODE	

  public static class Map extends MapReduceBase implements Mapper < LongWritable, Text, Text, IntWritable > {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException {
      String myvalue = value.toString();
      String[] tokens = myvalue.split(",");

      if (tokens[2].equals("30000")) {
        output.collect(new Text("Number of awards obtained by employee with salary of 30000: "), new IntWritable(Integer.parseInt(tokens[3])));
      }

    }
  }

  //REDUCER CODE	
  public static class Reduce extends MapReduceBase implements Reducer < Text, IntWritable, Text, IntWritable > {
    public void reduce(Text key, Iterator < IntWritable > values, OutputCollector < Text, IntWritable > output, Reporter reporter) throws IOException { //{little: {1,1}} 
      int total = 0;
      while (values.hasNext()) {
        total += values.next().get();
      }
      output.collect(key, new IntWritable(total));

    }
  }

  //DRIVER CODE
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Prog3.class);
    conf.setJobName("Prog3");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
  }
}
