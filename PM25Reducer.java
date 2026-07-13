import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PM25Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalPM25 = 0;
        int totalCount = 0;
        String stateFips = "";
        String countyFips = "";

        for (Text value : values) {

            String[] parts = value.toString().split(",");
            double pm25PopPred = Double.parseDouble(parts[0]);
            int count = Integer.parseInt(parts[3]);
            stateFips = parts[1];
            countyFips = parts[2];

            totalPM25 += pm25PopPred * count;
            totalCount += count;
        }

        double averagePM25 = totalPM25 / totalCount;
        context.write(key, new Text(stateFips + "," + countyFips + "," + totalCount + "," + averagePM25));
    }
}
