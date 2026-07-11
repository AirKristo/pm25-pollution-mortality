import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Formatted2019Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("year")) {
            return;
        }

        String[] parts = line.split(",");

        if (parts[0].equals("2019")) {

            if (parts[7] != null && !parts[7].trim().isEmpty()) {

                String formattedStateFips = String.format("%02d", Integer.parseInt(parts[2].trim()));

                String formattedCountyFips = String.format("%03d", Integer.parseInt(parts[3].trim()));

                String compositeKey = formattedStateFips + formattedCountyFips;

                parts[2] = formattedStateFips;
                parts[3] = formattedCountyFips;

                String modifiedLine = String.join(",", parts);

                context.write(new Text(compositeKey), new Text(modifiedLine));
            }
        }
    }
}