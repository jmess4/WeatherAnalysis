//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherjob;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import weatherwritables.WeatherWritable;

public class WeatherMapper extends Mapper<Object, Text, Text, WeatherWritable> {

	String line = null;
	Text year = new Text();
	WeatherWritable weatherWritable = new WeatherWritable();
	IntWritable numEntries = new IntWritable(0);
	boolean passedFirst = false;

	/*
	 * Input: Line of file, each line contains space(variable #) delimited
	 * fields of data Output: <Text, WeatherWritable> e.g. <1944, WeatherWritable>
	 */
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		line = value.toString();
		// first line, column headers, not used for analysis
		if (passedFirst == true) {
			year.set(line.substring(14, 18));
			weatherWritable.addMeanTemp(line.substring(24, 30));
			weatherWritable.addVisibility(line.substring(68, 73));
			weatherWritable.addMaxWindGust(line.substring(95, 100));
			weatherWritable.addMaxTemp(line.substring(102, 108));
			weatherWritable.addMinTemp(line.substring(110, 116));
		}
		passedFirst = true;
	}
	
	@Override
	public void cleanup(Context context) {
		try {
			context.write(year, weatherWritable);
		} catch (IOException e) {
			System.err.println(e.getMessage() + " While trying to write to context in Mapper");
		} catch (InterruptedException e) {
			System.err.println(e.getMessage() + " While trying to write to context in Mapper");
		}
	}
}
