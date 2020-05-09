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
			String arrestKey = lineArr[0].replace(' ', '-');
			String arrestDate = lineArr[1].replace(' ', '-');
			String PDCD = lineArr[2].replace(' ', '-');
			String offense1 = lineArr[3].replace(",", "-").replace(' ', '-');
			String KYCD = lineArr[4].replace(' ', '-');
			String offense2 = lineArr[5].replace(",", "-").replace(' ', '-');
			String lawCode = lineArr[6].replace(' ', '-');
			String lawCatCD = lineArr[7].replace(' ', '-');
			String borough = lineArr[8].replace(' ', '-');
			String precinct = lineArr[9].replace(' ', '-');
			String jurisdictionCode = lineArr[10].replace(' ', '-');
			String ageGroup = lineArr[11].replace(' ', '-'); 
			String gender = lineArr[12].replace(' ', '-');
			String race = lineArr[13].replace(' ', '-');
			String xCoord = lineArr[14].replace(' ', '-');
			String yCoord = lineArr[15].replace(' ', '-');
			String latitude = lineArr[16].replace(' ', '-');
			String longitude = lineArr[17].replace(' ', '-');	
		
			
//			if(gender.equals("M")){
//				gender = "L";	
//			}
			context.write(new Text(""), new Text(arrestKey + "," + arrestDate + "," + PDCD + "," + offense1  + "," + KYCD + "," + offense2 + "," + lawCode + "," + lawCatCD + "," + borough + "," + precinct + "," + jurisdictionCode + "," + ageGroup  + "," + gender + "," + race + "," + xCoord + "," + yCoord + "," + latitude + "," + longitude));
		}
	}
}