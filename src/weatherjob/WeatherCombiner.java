//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherjob;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weatherwritables.WeatherWritable;

public class WeatherCombiner extends
		Reducer<Text, WeatherWritable, Text, WeatherWritable> {

	WeatherWritable weatherWritable = new WeatherWritable();

	public void reduce(Text key, Iterable<WeatherWritable> values,
			Context context) throws IOException, InterruptedException {
		for (WeatherWritable wW : values) {
			weatherWritable.combineWeatherWritables(wW);
		}
		context.write(key, weatherWritable);
	}
}
