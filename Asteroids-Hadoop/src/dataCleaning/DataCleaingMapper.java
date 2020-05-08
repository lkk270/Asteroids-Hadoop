package dataCleaning;

import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DataCleaingMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable Key, Text value, Context con) throws IOException, InterruptedException{
		
	}
	
}
