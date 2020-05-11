import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.util.GenericOptionsParser;

public class BaseDriver extends Configured implements Tool{

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("config", args[3].split("=")[1]);

    Job job = Job.getInstance(conf, "count");
    job.setJarByClass(BaseDriver.class);
    
    job.setMapperClass(BaseMapper.class);
    job.setCombinerClass(BaseReducer.class);
    job.setReducerClass(BaseReducer.class);
    job.setNumReduceTasks(2);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    int ret = job.waitForCompletion(true) ? 0 : 1;

    return ret;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int exitCode = ToolRunner.run(conf, new BaseDriver(),  args);
    System.exit(exitCode);
  }
}