//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherjob;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weatherwritables.WeatherOutput;
import weatherwritables.WeatherWritable;

public class WeatherReducer extends
		Reducer<Text, WeatherWritable, Text, WeatherOutput> {

	WeatherOutput weatherOutput = new WeatherOutput();

	public void reduce(Text key, Iterable<WeatherWritable> values,
			Context context) throws IOException, InterruptedException {
		for (WeatherWritable wW : values) {
			weatherOutput.addEntry(wW);
		}
		weatherOutput.calculate();
		context.write(key, weatherOutput);
	}

}
