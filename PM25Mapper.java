import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PM25Mapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        String uniqueFips = parts[0].trim();
        String data = parts[1].trim();

        String[] columns = data.split(",");
        String stateFips = columns[2];
        String countyFips = columns[3];

        String pm25PopPred = columns[columns.length - 1].trim();

        context.write(new Text(uniqueFips), new Text(pm25PopPred + "," + stateFips + "," + countyFips + ",1"));

    }

}
