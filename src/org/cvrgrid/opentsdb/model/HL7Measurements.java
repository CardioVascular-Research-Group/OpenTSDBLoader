package org.cvrgrid.opentsdb.model;

import java.util.HashMap;

public class HL7Measurements {

	private final long serialVersionUID = 6721093052749142738L;
	private HashMap<String,String> measurementNames = new HashMap<String,String>();

	public HL7Measurements() {

	}

	public HashMap<String,String> getMeasurementNames() {
		return measurementNames;
	}

	public void setMeasurementNames(HashMap<String,String> measurementNames) {
		this.measurementNames = measurementNames;
	}

}
