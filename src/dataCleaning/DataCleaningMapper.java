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
	private Text word = new Text();
	
	public void map(LongWritable _key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArr = value.toString().split("\t");
		if(lineArr.length == 18 && checkBlank(lineArr) == false) {
			//String arrestKey = lineArr[0].replace(' ', '-');
			String arrestDate = lineArr[1].replace(' ', '-');
			//String PDCD = lineArr[2].replace(' ', '-');
			String offense1 = lineArr[3].replace(",", "-").replace(' ', '-').toUpperCase();
			//String KYCD = lineArr[4].replace(' ', '-');
			String offense2 = lineArr[5].replace(",", "-").replace(' ', '-').toUpperCase();
			//String lawCode = lineArr[6].replace(' ', '-');
			//String lawCatCD = lineArr[7].replace(' ', '-');
			String borough = lineArr[8].replace(' ', '-').toUpperCase();
			String precinct = lineArr[9].replace(' ', '-');
			String jurisdictionCode = lineArr[10].replace(' ', '-');
			String ageGroup = lineArr[11].replace(' ', '-'); 
			String gender = lineArr[12].replace(' ', '-').toUpperCase();
			String race = lineArr[13].replace(' ', '-').toUpperCase();
			//String xCoord = lineArr[14].replace(' ', '-');
			//String yCoord = lineArr[15].replace(' ', '-');
			//String latitude = lineArr[16].replace(' ', '-');
			//String longitude = lineArr[17].replace(' ', '-');	
			if(checkAll(gender, borough, ageGroup, precinct, jurisdictionCode)){
				try{
					String year = arrestDate.split("/")[2];
					offense1 = formatOffense(offense1);
					offense2 = formatOffense(offense2);
	
					context.write(new Text(""), new Text(arrestDate + "," + year + ","   + offense1  + "," + offense2  + "," + borough + "," + precinct + "," + jurisdictionCode + "," + ageGroup  + "," + gender + "," + race));
				}
				catch(Exception e){
					
				}
			}
			
		}
	}


	public String formatOffense(String offense){
		String formattedOffense;

		if(offense.contains("WEAPON") || offense.contains("FIREARM")){
			formattedOffense = "Weapon";
		}
		else if(offense.contains("MARIJUANA")){
			formattedOffense = "Marijuana";
		}
		else if(offense.contains("SEX") || offense.contains("SODOMY") || offense.contains("RAPE") || 
		offense.contains("PROSTITUTION") || offense.contains("INCEST") || offense.contains("EXPOSURE") || 
		offense.contains("OBSCENITY") || offense.contains("LEWD") || offense.contains("INTIMATE")){
			formattedOffense = "Sexual-Misconduct/Abuse";
		}
		else if((offense.contains("WHILE") && offense.contains("DRIV")) || offense.contains("BELTS") || offense.contains("SPEEDING") || 
		offense.contains("OF-WAY") || offense.contains("PARKING") || offense.contains("ACCIDENT") || offense.contains("FAIL-TO-STOP") || 
		(offense.contains("INTOX") && offense.contains("DRIV")) || offense.contains("ONE-WAY") || offense.contains("LIGHTS") || 
		offense.contains("TRAFFIC-") || offense.contains("LEAVING-SCENE-ACCIDENT") || offense.contains("IMPAIRED-DRIVING") || 
		offense.contains("IMPROPER") || offense.contains("FAIL-TO-STOP")){
			formattedOffense = "Reckless-Driving";
		}
		else if((offense.contains("UNLICENSED") && offense.contains("OPERATOR")) || (offense.contains("UNAUTHORIZED") && offense.contains("VEHICLE"))){
			formattedOffense = "Unlicensed/Unauthorized-Driver";
		}
		else if(offense.contains("TRESPASS")){
			formattedOffense = "Trespassing";
		}
		else if(offense.contains("TERROR") || offense.contains("CONSPIRACY")){
			formattedOffense = "Terrorism";
		}
		else if(offense.contains("TAMPERING")){
			formattedOffense = "Tampering";
		}
		else if(offense.contains("ROBBERY") || offense.contains("THEFT") || offense.contains("LARCENY") || 
		offense.contains("BURGLARY") || offense.contains("STOLEN-PROPERTY") || offense.contains("PEDDLING")){
			formattedOffense = "Robbery/Theft/Larceny";
		}
		else if(offense.contains("FORGE") || offense.contains("FRAUD") || offense.contains("BRIBERY") || 
		offense.contains("MANUFACTURE-UNAUTHORIZED-RECORDINGS") || offense.contains("IDENTITY-THFT")){
			formattedOffense = "Forgery/Fraud";
		}
		else if(offense.contains("RECKLESS-ENDANGERMENT") || offense.contains("RIOT")){
			formattedOffense = "Reckless-Endangerment";
		}
		else if(offense.contains("GAMBLING")){
			formattedOffense = "Gambling";
		}
		else if(offense.contains("MISCHIEF") || offense.contains("MENACING") || offense.contains("NUISANCE") || offense.contains("COERCION") || 
		offense.contains("DISORDERLY-CONDUCT") || offense.contains("NOISE-UNECESSARY") || offense.contains("LOITERING") || offense.contains("ARSON") || 
		offense.contains("ALCOHOLIC") || offense.contains("SOLICITATION") || offense.contains("DISORDERLY-CONDUCT") || offense.contains("FIREWORKS") || 
		offense.contains("MENACING")){
			formattedOffense = "Mischief";
		}
		else if(offense.contains("MURDER") || offense.contains("HOMICIDE") || offense.contains("MANSLAUGHTER")){
			formattedOffense = "Murder";
		}
		else if(offense.contains("CONTROLLED-SUBSTANCE") || offense.contains("SALE-SCHOOL-GROUNDS") || 
		offense.contains("DRUG") || offense.contains("HYPODERMIC") || offense.contains("METH")){
			formattedOffense = "Controlled-Substance";
		}
		else if(offense.contains("HARASSMENT") || offense.contains("ASSAULT") || offense.contains("STRANGULATION")){
			formattedOffense = "Assault";
		}
		else if(offense.contains("CONTEMPT")){
			formattedOffense = "Contempt";
		}
		else if(offense.contains("CHILD") || offense.contains("KIDNAPPING")){
			formattedOffense = "Child-Endangerment";
		}
		else if(offense.contains("BAIL")){
			formattedOffense = "Bail-Jumping";
		}
		else if(offense.contains("RESISTING-ARREST")){
			formattedOffense = "Resisting-Arrest";
		}
		else if(offense.contains("IMPERSONATION")){
			formattedOffense = "Impersonation";
		}
		else if(offense.contains("MISDEMEANOR")){
			formattedOffense = "Misdemeanor";
		}
		else{
			formattedOffense = "Unclassified/Other";
		}

		return formattedOffense;	
	}


	public boolean checkBlank(String[] arr){
		boolean blank = false;
		int [] indices = {1, 3, 5, 8, 9, 10, 11, 12, 13};
		for (int i = 0; i < indices.length; i++){
			int index =  indices[i];
			if(arr[index].length() == 0 || arr[index].equals(" ")){
				return true;
			}
		}
		return blank;
	}


	public boolean checkGender(String gender){
		boolean good = false;
		if (gender.equals("M") || gender.equals("F")){
			good = true;
		}
		return good;
	}


	public boolean checkBorough(String borough){
		boolean good = false;
		if (borough.equals("M") || borough.equals("B") || borough.equals("K") || borough.equals("S") || borough.equals("Q")){
			good = true;
		}
		return good;
	}
	

	public boolean checkAge(String age){
		boolean good = false;
		if (age.equals("<18") || age.equals("18-24") || age.equals("25-44") || age.equals("45-64") || age.equals("65+")){
			good = true;
		}
		return good;
	}


	public boolean checkPrecinctAndJurisdiction(String val){
		try {
			int number = Integer.parseInt(val);
			return true;
		} 
		catch(NumberFormatException e) {
			return false;
		}
	}


	public boolean checkAll(String gender, String borough, String age, String precinct, String jurisdiction){
		boolean good = false;
		if(checkGender(gender) && checkBorough(borough) && checkAge(age) && checkPrecinctAndJurisdiction(precinct) && checkPrecinctAndJurisdiction(jurisdictionCode)){
			good = true;
		}
		return good;
	}
}