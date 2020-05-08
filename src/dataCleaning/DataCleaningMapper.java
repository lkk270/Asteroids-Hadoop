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


public class DataCleaningMapper extends Mapper<LongWritable, Text, Text, Text>{
//	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
public void map(LongWritable _key, Text value, Context context) throws IOException, InterruptedException {
//		StringTokenizer itr = new StringTokenizer(value.toString().split("\t")[12]);
//		while (itr.hasMoreTokens()) {
//			word.set(itr.nextToken());
//			context.write(new Text(word), one);
//		}
		String[] lineArr = value.toString().split("\t");
		if(lineArr.length == 18) {
			String arrestKey = lineArr[0];
			String arrestDate = lineArr[1];
			String PDCD = lineArr[2];
			String offense1 = lineArr[3].replace(",", "-");
			String KYCD = lineArr[4];
			String offense2 = lineArr[5].replace(",", "-");
			String lawCode = lineArr[6];
			String lawCatCD = lineArr[7];
			String borough = lineArr[8];
			String precinct = lineArr[9];
			String jurisdictionCode = lineArr[10];
			String ageGroup = lineArr[11]; 
			String gender = lineArr[12];
			String race = lineArr[13];
			String xCoord = lineArr[14];
			String yCoord = lineArr[15];
			String latitude = lineArr[16];
			String longitude = lineArr[17];	
		
			
//			if(gender.equals("M")){
//				gender = "L";	
//			}
			context.write(new Text(""), new Text(arrestKey + "," + arrestDate + "," + offense1 + "," + PDCD + "," + KYCD + "," + offense2 + "," + lawCode + "," + lawCatCD + "," + borough + "," + precinct + "," + jurisdictionCode + "," + ageGroup  + "," + gender + "," + race + "," + xCoord + "," + yCoord + "," + latitude + "," + longitude));
		}
	}
}