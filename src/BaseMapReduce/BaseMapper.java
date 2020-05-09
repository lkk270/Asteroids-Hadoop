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


public class BaseMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable _key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString().split(",")[3]);
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(new Text(word), one);
		}
		
	}
}