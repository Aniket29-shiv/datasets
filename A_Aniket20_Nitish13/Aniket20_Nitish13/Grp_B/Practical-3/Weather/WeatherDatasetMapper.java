package Weather;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WeatherDatasetMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String year_temp = line.substring(15, 19) + "_temp";
        String year_dew = line.substring(15, 19) + "_dew";
        String year_wind = line.substring(15, 19) + "_wind";
        int airTemperature, dewpoint, windspeed;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        if (line.charAt(93) == '+') {
            dewpoint = Integer.parseInt(line.substring(94, 98));
        } else {
            dewpoint = Integer.parseInt(line.substring(93, 98));
        }

        windspeed = Integer.parseInt(line.substring(60, 63));
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            output.collect(new Text(year_temp), new IntWritable(airTemperature));
            output.collect(new Text(year_dew), new IntWritable(dewpoint));
            output.collect(new Text(year_wind), new IntWritable(windspeed));
        }
    }
}