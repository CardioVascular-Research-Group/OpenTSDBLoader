package org.cvrgrid.opentsdb.model;

import java.util.ArrayList;
import java.util.HashMap;

public class Metric {

	/** The incoming metric name */
	private String metric;

	/** A hash map of tag name/values */
	private HashMap<String, String> tags;

	/** A list of tag name/values */
	private ArrayList<String> aggregateTags;

	/** The incoming timestamps in Unix epoch seconds or milliseconds with values*/
	private HashMap<String, String> dps;


	/**
	 * Empty constructor necessary for some de/serializers
	 */
	public Metric() {

	}

	public Metric(final String metric,
			final HashMap<String, String> tags,
			final ArrayList<String> aggregateTags,
			final HashMap<String, String> dps) {
		this.metric = metric;
		this.tags = tags;
		this.aggregateTags = aggregateTags;
		this.dps = dps;
	}

	/**
	 * @return the metric
	 */
	public String getMetric() {
		return metric;
	}


	/**
	 * @param metric the metric to set
	 */
	public void setMetric(String metric) {
		this.metric = metric;
	}


	/**
	 * @return the tags
	 */
	public HashMap<String, String> getTags() {
		return tags;
	}


	/**
	 * @param tags the tags to set
	 */
	public void setTags(HashMap<String, String> tags) {
		this.tags = tags;
	}


	/**
	 * @return the aggregatedTags
	 */
	public ArrayList<String> getAggregateTags() {
		return aggregateTags;
	}


	/**
	 * @param aggregatedTags the aggregatedTags to set
	 */
	public void setAggregateTags(ArrayList<String> aggregateTags) {
		this.aggregateTags = aggregateTags;
	}


	/**
	 * @return the dps
	 */
	public HashMap<String, String> getDps() {
		return dps;
	}


	/**
	 * @param dps the dps to set
	 */
	public void setDps(HashMap<String, String> dps) {
		this.dps = dps;
	}

	public String toString() {
		String output = "Metric: " + metric + " aggregateTags: " + aggregateTags.size();
		return output;
	}
}
