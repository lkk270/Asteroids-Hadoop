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


public class FormatDataMapper extends Mapper<LongWritable, Text, Text, Text>{
//	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
public void map(LongWritable _key, Text value, Context context) throws IOException, InterruptedException {
//		StringTokenizer itr = new StringTokenizer(value.toString().split("\t")[12]);
//		while (itr.hasMoreTokens()) {
//			word.set(itr.nextToken());
//			context.write(new Text(word), one);
//		}
		String[] lineArr = value.toString().split(",");
		if(lineArr.length == 18) {
			String offense1 = lineArr[3];
			String offense2 = lineArr[5];
			
			if offense1.contains("s")
		
			
//			if(gender.equals("M")){
//				gender = "L";	
//			}
			context.write(new Text(""), new Text(arrestKey + "," + arrestDate + "," + offense1 + "," + PDCD + "," + KYCD + "," + offense2 + "," + lawCode + "," + lawCatCD + "," + borough + "," + precinct + "," + jurisdictionCode + "," + ageGroup  + "," + gender + "," + race + "," + xCoord + "," + yCoord + "," + latitude + "," + longitude));
		}
	}
}