//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherwritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class WeatherWritable implements Writable {

	private FloatWritable maxTemp = new FloatWritable(0);
	private IntWritable numMaxTempEntries = new IntWritable(0);
	private FloatWritable meanTemp = new FloatWritable(0);
	private IntWritable numMeanTempEntries = new IntWritable(0);
	private FloatWritable minTemp = new FloatWritable(0);
	private IntWritable numMinTempEntries = new IntWritable(0);
	private FloatWritable maxWindGust = new FloatWritable(0);
	private IntWritable numMaxWindEntries = new IntWritable(0);
	private FloatWritable visibility = new FloatWritable(0);
	private IntWritable numVisibilityEntries = new IntWritable(0);
	private FloatWritable[] recordMinTemps = new FloatWritable[10];
	private FloatWritable[] recordMaxTemps = new FloatWritable[10];

	public WeatherWritable() {
		for (int i = 0; i < 10; i++) {
			recordMinTemps[i] = new FloatWritable(Float.MAX_VALUE);
			recordMaxTemps[i] = new FloatWritable(Float.MIN_VALUE);
		}
	}

	public void combineWeatherWritables(WeatherWritable rhs) {
		this.maxTemp.set(this.getMaxTemp() + rhs.getMaxTemp());
		this.numMaxTempEntries.set(this.getNumMaxTempEntries()
				+ rhs.getNumMaxTempEntries());
		this.meanTemp.set(this.getMeanTemp() + rhs.getMeanTemp());
		this.numMeanTempEntries.set(this.getNumMeanTempEntries()
				+ rhs.getNumMeanTempEntries());
		this.minTemp.set(this.getMinTemp() + rhs.getMinTemp());
		this.numMinTempEntries.set(this.getNumMinTempEntries()
				+ rhs.getNumMinTempEntries());
		this.maxWindGust.set(this.getMaxWindGust() + rhs.getMaxWindGust());
		this.numMaxWindEntries.set(this.getNumMaxWindEntries()
				+ rhs.getNumMaxWindEntries());
		this.visibility.set(this.getVisibility() + rhs.getVisibility());
		this.numVisibilityEntries.set(this.getNumVisibilityEntries()
				+ rhs.getNumVisibilityEntries());
		float[] copyOfRecords = rhs.getRecordMinTemps();
		for (int i = 0; i < 10; i++) {
			if (copyOfRecords[i] < this.recordMinTemps[9].get()) {
				recordMinTemps[9].set(copyOfRecords[i]);
				Arrays.sort(this.recordMinTemps);
			}
		}
		copyOfRecords = rhs.getRecordMaxTemps();
		for (int i = 0; i < 10; i++) {
			if (copyOfRecords[i] > this.recordMaxTemps[0].get()) {
				recordMaxTemps[0].set(copyOfRecords[i]);
				Arrays.sort(this.recordMaxTemps);
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		maxTemp.readFields(in);
		meanTemp.readFields(in);
		minTemp.readFields(in);
		maxWindGust.readFields(in);
		visibility.readFields(in);
		numMaxTempEntries.readFields(in);
		numMeanTempEntries.readFields(in);
		numMinTempEntries.readFields(in);
		numMaxWindEntries.readFields(in);
		numVisibilityEntries.readFields(in);
		for (int i = 0; i < 10; i++) {
			recordMinTemps[i].readFields(in);
			recordMaxTemps[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		maxTemp.write(out);
		meanTemp.write(out);
		minTemp.write(out);
		maxWindGust.write(out);
		visibility.write(out);
		numMaxTempEntries.write(out);
		numMeanTempEntries.write(out);
		numMinTempEntries.write(out);
		numMaxWindEntries.write(out);
		numVisibilityEntries.write(out);
		for (int i = 0; i < 10; i++) {
			recordMinTemps[i].write(out);
			recordMaxTemps[i].write(out);
		}
	}

	public float getMaxTemp() {
		return maxTemp.get();
	}

	public void addMaxTemp(String maxTemp) {
		if ( ! (maxTemp).equals("9999.9") ) {
			float passedValue = Float.parseFloat(maxTemp);
			this.maxTemp.set(this.maxTemp.get() + passedValue);
			this.numMaxTempEntries.set(this.numMaxTempEntries.get() + 1);
			if (passedValue > recordMaxTemps[0].get()) {
				recordMaxTemps[0].set(passedValue);
				Arrays.sort(recordMaxTemps);
			}
		}
		
	}

	public float getMeanTemp() {
		return meanTemp.get();
	}

	public void addMeanTemp(String meanTemp) {
		if ( ! (meanTemp).equals("9999.9") ) {
			float passedValue = Float.parseFloat(meanTemp);
			this.meanTemp.set(this.meanTemp.get() + passedValue);
			this.numMeanTempEntries.set(this.numMeanTempEntries.get() + 1);
		}
	}

	public float getMinTemp() {
		return minTemp.get();
	}

	public void addMinTemp(String minTemp) {
		if ( ! (minTemp).equals("9999.9") ) {
			float passedValue = Float.parseFloat(minTemp);
			this.minTemp.set(this.minTemp.get() + passedValue);
			this.numMinTempEntries.set(this.numMinTempEntries.get() + 1);
			if (passedValue < recordMinTemps[9].get()) {
				recordMinTemps[9].set(passedValue);
				Arrays.sort(recordMinTemps);
			}
		}
		
	}

	public float getMaxWindGust() {
		return maxWindGust.get();
	}

	public void addMaxWindGust(String maxWindGust) {
		if ( ! (maxWindGust).equals("999.9") ) {
			float passedValue = Float.parseFloat(maxWindGust);
			this.maxWindGust.set(this.maxWindGust.get() + passedValue);
			this.numMaxWindEntries.set(this.numMaxWindEntries.get() + 1);
		}
	}

	public float getVisibility() {
		return visibility.get();
	}

	public void addVisibility(String visibility) {
		if ( ! (visibility).equals("999.9") ) {
			float passedValue = Float.parseFloat(visibility);
			this.visibility.set(this.visibility.get() + passedValue);
			this.numVisibilityEntries.set(this.numVisibilityEntries.get() + 1);
		}
	}

	public int getNumMaxTempEntries() {
		return numMaxTempEntries.get();
	}

	public int getNumMeanTempEntries() {
		return numMeanTempEntries.get();
	}

	public int getNumMinTempEntries() {
		return numMinTempEntries.get();
	}

	public int getNumMaxWindEntries() {
		return numMaxWindEntries.get();
	}

	public int getNumVisibilityEntries() {
		return numVisibilityEntries.get();
	}

	public float[] getRecordMinTemps() {
		float[] copy = new float[10];
		for (int i = 0; i < 10; i++) {
			copy[i] = recordMinTemps[i].get();
		}
		return copy;
	}

	public float[] getRecordMaxTemps() {
		float[] copy = new float[10];
		for (int i = 0; i < 10; i++) {
			copy[i] = recordMaxTemps[i].get();
		}
		return copy;
	}

}
