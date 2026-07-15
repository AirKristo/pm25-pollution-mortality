import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DeathCategoryWritable implements WritableComparable
{
	private IntWritable stateFIPS;
	private IntWritable countyFIPS;
	private Text race;
	private Text ageGroup;
	private IntWritable year;

	public DeathCategoryWritable()
	{
		stateFIPS = new IntWritable();
		countyFIPS = new IntWritable();
		race = new Text();
		ageGroup = new Text();
		year = new IntWritable();
	}

	public DeathCategoryWritable(IntWritable stateFIPS, IntWritable countyFIPS, Text race, Text ageGroup, IntWritable year)
	{
		this.stateFIPS = stateFIPS;
		this.countyFIPS = countyFIPS;
		this.race = race;
		this.ageGroup = ageGroup;
		this.year = year;
	}

	public IntWritable getStateFIPS()
	{
		return stateFIPS;
	}

	public IntWritable getCountyFIPS()
	{
		return countyFIPS;
	}

	public Text getRace()
	{
		return race;
	}

	public Text getAgeGroup()
	{
		return ageGroup;
	}

	public IntWritable getYear()
	{
		return year;
	}

	public void readFields(DataInput in) throws IOException
	{
		stateFIPS.readFields(in);
		countyFIPS.readFields(in);
		race.readFields(in);
		ageGroup.readFields(in);
		year.readFields(in);
	}

	public void write(DataOutput out) throws IOException
	{
		stateFIPS.write(out);
		countyFIPS.write(out);
		race.write(out);
		ageGroup.write(out);
		year.write(out);
	}

	@Override
	public String toString()
	{
		return stateFIPS.toString() + "," + countyFIPS.toString() + "," + year.toString() + "," + race.toString() + "," + ageGroup.toString()+",";
	}

    	@Override
    	public int compareTo(Object o)
	{

		if(o instanceof DeathCategoryWritable)
		{
			DeathCategoryWritable dcw = (DeathCategoryWritable) o;
			int cmp = stateFIPS.compareTo(dcw.stateFIPS);
			if(cmp == 0) {cmp = countyFIPS.compareTo(dcw.countyFIPS);}
			if(cmp == 0) {cmp = year.compareTo(dcw.year);}
			if(cmp == 0) {cmp = race.compareTo(dcw.race);}
			if(cmp == 0) {cmp = ageGroup.compareTo(dcw.ageGroup);}
			return cmp;
		}
		return -1;
    	}
 
    	@Override
    	public int hashCode()
	{
        	int hash = 7;
		hash = 31 * hash + stateFIPS.get();
		hash = 31 * hash + countyFIPS.get();
		hash = 31 * hash + year.get();
		hash = 31 * hash + race.hashCode();
		return 31 * hash + ageGroup.hashCode();
    	}
 
    	@Override
    	public boolean equals(Object o)
    	{
        	if(o instanceof DeathCategoryWritable)
        	{
            		DeathCategoryWritable dcw = (DeathCategoryWritable) o;
            		boolean eq = stateFIPS.equals(dcw.stateFIPS); 
			eq = eq && countyFIPS.equals(dcw.countyFIPS);
			eq = eq && year.equals(dcw.year);
			eq = eq && race.equals(dcw.race);
			return eq && ageGroup.equals(dcw.ageGroup);
        	}
        	return false;
    	}	
}
