package Weather;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WeatherDatasetReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
        int tempValue = 0, dewValue = 0, windSpeedValue = 0, countTemp = 0, countDew = 0, countWindSpeed = 0, tempAvg,
                dewAvg, windSpeedAvg;
        String key_str = key.toString();
        if (key_str.contains("_temp")) {
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                IntWritable value = (IntWritable) values.next();

                tempValue += value.get();
                countTemp += 1;
            }
            tempAvg = tempValue / countTemp;
            output.collect(key, new IntWritable(tempAvg));
        } else if (key_str.contains("_dew")) {
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                IntWritable value = (IntWritable) values.next();
                dewValue += value.get();
                countDew += 1;
            }
            dewAvg = dewValue / countDew;
            output.collect(key, new IntWritable(dewAvg));
        } else {
            while (values.hasNext()) {
                // replace type of value with the actual type of our value
                IntWritable value = (IntWritable) values.next();
                windSpeedValue += value.get();
                countWindSpeed += 1;
            }
            windSpeedAvg = windSpeedValue / countWindSpeed;
            output.collect(key, new IntWritable(windSpeedValue));
        }
    }
}
