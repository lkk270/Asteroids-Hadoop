import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.JobConf;


public class BaseMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private String index;

	@Override
    protected void setup(Mapper.Context context)throws IOException, InterruptedException {
        // get the searchingWord from configuration
		index = context.getConfiguration().get("config");
		System.out.print("INDEX " + index);
    }
	
	public void map(LongWritable _key, Text value, Context context) throws IOException, InterruptedException {
		// Configuration conf = context.getConfiguration();
		// String index = conf.get("my.dummy.configuration");
		StringTokenizer itr = new StringTokenizer(value.toString().split(",")[Integer.parseInt(index)]);
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(new Text(word), one);
		}
		
	}
}