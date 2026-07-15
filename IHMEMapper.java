import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IHMEMapper extends Mapper<LongWritable, Text, DeathCategoryWritable, DoubleWritable>
{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] tokens = line.split(",",-1);
		if(tokens[0].equals("1") && tokens[1].equals("Deaths") //Check we have deaths data
			&& tokens[4].length() > 2 //Ensure FIPS is county level data
			&& tokens[6].length() > 0 //Ensure valid race data
			&& tokens[7].equals("3") && tokens[8].equals("Both") //Check we have all genders
			&& tokens[10].length() > 0 //Ensure valid age data
			&& tokens[11].equals("294") && tokens[12].equals("All causes") //Check All causes data
			&& tokens[13].length() > 0 //Ensure valid year data
			&& tokens[14].equals("3") && tokens[15].equals("Rate") //Check Death Rate data)
			&& tokens[16].length() > 0 //Ensure valid death rate data
			&& tokens.length == 19)
		{
			int FIPS = Integer.parseInt(tokens[4]);
			IntWritable stateCode = new IntWritable(FIPS/1000);
			IntWritable countyCode = new IntWritable(FIPS%1000);
			Text race = new Text(tokens[6]);
			Text age = new Text(tokens[10]);
			IntWritable year = new IntWritable(Integer.parseInt(tokens[13]));
			DoubleWritable deathRate = new DoubleWritable(Double.parseDouble(tokens[16]));
			context.write(new DeathCategoryWritable(stateCode, countyCode, race, age, year), deathRate);
		}
	}
}
