//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherjob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weatherwritables.WeatherOutput;
import weatherwritables.WeatherWritable;

public class WeatherJob {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WeatherData");

		job.setJarByClass(WeatherJob.class);
		job.setMapperClass(WeatherMapper.class);
		job.setCombinerClass(WeatherCombiner.class);
		job.setReducerClass(WeatherReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeatherWritable.class);

		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WeatherOutput.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}