//Author: Jordan Messec
//Date: 4/29/15
//Email: jmess4@gmail.com
package weatherwritables;

import java.util.Arrays;

public class WeatherOutput {

	private float avgMaxTemp = 0;
	private int numMaxTempEntries = 0;
	private float avgMeanTemp = 0;
	private int numMeanTempEntries = 0;
	private float avgMinTemp = 0;
	private int numMinTempEntries = 0;
	private float avgMaxWindGust = 0;
	private int numMaxWindGustEntries = 0;
	private float avgVisibility = 0;
	private int numVisibilityEntries = 0;
	private float[] recordMinTemps = new float[10];
	private float[] recordMaxTemps = new float[10];

	public WeatherOutput() {
		for (int i = 0; i < 10; i++) {
			recordMinTemps[i] = Float.MAX_VALUE;
			recordMaxTemps[i] = Float.MIN_VALUE;
		}
	}

	//used to sum together daily values
	public void addEntry(WeatherWritable wW) {
		avgMaxTemp += wW.getMaxTemp();
		numMaxTempEntries += wW.getNumMaxTempEntries();
		avgMeanTemp += wW.getMeanTemp();
		numMeanTempEntries += wW.getNumMeanTempEntries();
		avgMinTemp += wW.getMinTemp();
		numMinTempEntries += wW.getNumMinTempEntries();
		avgMaxWindGust += wW.getMaxWindGust();
		numMaxWindGustEntries += wW.getNumMaxWindEntries();
		avgVisibility += wW.getVisibility();
		numVisibilityEntries += wW.getNumVisibilityEntries();
		float[] copyOfRecords = wW.getRecordMinTemps();
		for (int i = 0; i < 10; i++) {
			if (copyOfRecords[i] < this.recordMinTemps[9]) {
				recordMinTemps[9] = copyOfRecords[i];
				Arrays.sort(this.recordMinTemps);
			}
		}
		copyOfRecords = wW.getRecordMaxTemps();
		for (int i = 0; i < 10; i++) {
			if (copyOfRecords[i] > this.recordMaxTemps[0]) {
				recordMaxTemps[0] = copyOfRecords[i];
				Arrays.sort(this.recordMaxTemps);
			}
		}
	}

	// calculates avgs after all entries summed
	public void calculate() {
		avgMaxTemp /= numMaxTempEntries;
		avgMeanTemp /= numMeanTempEntries;
		avgMinTemp /= numMinTempEntries;
		avgMaxWindGust /= numMaxWindGustEntries;
		avgVisibility /= numVisibilityEntries;
	}

	@Override
	public String toString() {
		String topTenTemps = "";
		for (int i = 0; i < 10; i++) {
			topTenTemps += recordMaxTemps[i] + "F, ";
		}
		topTenTemps = topTenTemps.substring(0, topTenTemps.length() - 2);
		String lowestTenTemps = "";
		for (int i = 0; i < 10; i++) {
			lowestTenTemps += recordMinTemps[i] + "F, ";
		}
		lowestTenTemps = lowestTenTemps.substring(0,
				lowestTenTemps.length() - 2);
		return "Max Temp Avg: " + avgMaxTemp + "F\t" + "Mean Temp Avg: "
				+ avgMeanTemp + "F\t" + "Min Temp Avg: " + avgMinTemp + "F\t"
				+ "Max Wind Gust Avg: " + avgMaxWindGust + "knots\t"
				+ "Visibility Avg: " + avgVisibility + "miles\t"
				+ "Highest 10 Recorded Temps: " + topTenTemps + "\t"
				+ "Lowest 10 Recorded Temps: " + lowestTenTemps;
	}

}
