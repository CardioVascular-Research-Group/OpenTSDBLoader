package org.cvrgrid.opentsdb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.cvrgrid.opentsdb.model.IncomingDataPoint;
import org.cvrgrid.opentsdb.model.OpenTSDBConfiguration;

import com.google.gson.Gson;


public class SubjectDataLoader {

	private String configFilename = "/resources/server.properties";
	private OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();

	/**
	 * Constructor for this code intended to set all the variables based upon the properties file.
	 */
	public SubjectDataLoader(){		 

		try {

			OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();
			Properties serverProperties = new Properties();
			InputStream stream = SubjectDataLoader.class.getResourceAsStream(this.getConfigFilename());
			serverProperties.load(stream);
			openTSDBConfiguration.setOpenTSDBUrl(serverProperties.getProperty("openTSDBUrl"));
			openTSDBConfiguration.setApiPut(serverProperties.getProperty("apiPut"));
			openTSDBConfiguration.setApiQuery(serverProperties.getProperty("apiQuery"));
			openTSDBConfiguration.setAwareSupportedParams(serverProperties.getProperty("awareSupportedParams"));
			openTSDBConfiguration.setIdMatch(serverProperties.getProperty("idMatch"));
			openTSDBConfiguration.setIdMatchSheet(serverProperties.getProperty("idMatchSheet"));
			openTSDBConfiguration.setRootDir(serverProperties.getProperty("rootDir"));
			openTSDBConfiguration.setFolderPath(serverProperties.getProperty("folderPath"));
			openTSDBConfiguration.setStudyString(serverProperties.getProperty("studyString"));
			this.setOpenTSDBConfiguration(openTSDBConfiguration);

		} catch (IOException e) {

			e.printStackTrace();

		}

	}

	public static void main(String[] args) throws Exception {

		SubjectDataLoader subjectDataLoader = new SubjectDataLoader();
		OpenTSDBConfiguration openTSDBConfiguration = subjectDataLoader.getOpenTSDBConfiguration();
		ArrayList<String> subjectHashes = new ArrayList<String>();
		Gson gson = new Gson();
		XSSFWorkbook wb = readFile(openTSDBConfiguration.getIdMatch());
		XSSFSheet sheet = wb.getSheet(openTSDBConfiguration.getIdMatchSheet());
		for (int r = 1; r < sheet.getLastRowNum()+1; r++) {
			XSSFRow row = sheet.getRow(r);
			if (row == null) {
				continue;
			}
			subjectHashes.add(row.getCell(2).getStringCellValue());
		}
		int subjectCount = 0;
		for (String subjectHash : subjectHashes) {
			HashMap<String,Hashtable<Date, Double>> subjectData = new HashMap<String,Hashtable<Date, Double>>();
			Hashtable<Date, Double> timeSeries = new Hashtable<Date, Double>();
			HashMap<String,String> tags = new HashMap<String,String>();
			tags.put("subjectHash", subjectHash);
			SimpleDateFormat fromExcel = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			wb = readFile(openTSDBConfiguration.getFolderPath() + subjectHash + ".xlsx");
			for (int i = 0; i < wb.getNumberOfSheets(); i++) {
				XSSFSheet sheetIn = wb.getSheetAt(i);
				for (int r = 1; r <= sheetIn.getLastRowNum(); r++) {
					XSSFRow row = sheetIn.getRow(r);	
					Date reformattedTime = fromExcel.parse(row.getCell(0).getStringCellValue());
					timeSeries.put(reformattedTime, row.getCell(1).getNumericCellValue());
				}
				subjectData.put(sheetIn.getSheetName(), timeSeries);
				timeSeries = new Hashtable<Date, Double>();
			}
			Set<String> keys = subjectData.keySet();
			TreeSet<String> sortedKeys = new TreeSet<String>(keys);
			String subjectId = openTSDBConfiguration.getStudyString();
			int number = ++subjectCount;
			if (number < 10) {
				subjectId += "0000" + new Integer(number).toString();
			} else if (number < 100) {
				subjectId += "000" + new Integer(number).toString();
			} else if (number < 1000) {
				subjectId += "00" + new Integer(number).toString();
			} else if (number < 10000) {
				subjectId += "0" + new Integer(number).toString();
			} else {
				subjectId += new Integer(number).toString();						
			}
			tags.put("subjectId", subjectId);
			for (String key : sortedKeys) {
				String urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiPut();
				URL url = new URL(urlString);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Accept", "application/json");
				conn.setRequestProperty("Content-type", "application/json");
				conn.setDoOutput(true);
				conn.setDoInput(true);
				OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
				String[] variableAndUnits = key.split("\\(");
				String keyReplace = variableAndUnits[0];
				keyReplace = keyReplace.replaceAll(" ", "");
				if (keyReplace.equalsIgnoreCase("PulseOximetryPeripheralHeart"))
					keyReplace += "Rate";
				if (variableAndUnits.length > 1) {
					if (variableAndUnits.length > 2)
						variableAndUnits[1] += variableAndUnits[2];
					variableAndUnits[1] = variableAndUnits[1].replaceAll("\\)", "");
					variableAndUnits[1] = variableAndUnits[1].replaceAll(" ", "");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("%", "percent");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("#", "count");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("mmhg", "mmHg");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("breathsperm", "breaths");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("breaths", "breathspermin");
					variableAndUnits[1] = variableAndUnits[1].replaceAll("cel", "Celsius");					
					tags.put("units", variableAndUnits[1]);
					if (variableAndUnits[0].equalsIgnoreCase("Pulse Oximetry Peripheral Heart"))
						tags.put("units", "per min");
				}
				Set<Date> timePoints = subjectData.get(key).keySet();
				TreeSet<Date> sortedTimePoints = new TreeSet<Date>(timePoints);
				ArrayList<IncomingDataPoint> points = new ArrayList<IncomingDataPoint>();
				for (Date time : sortedTimePoints) {
					IncomingDataPoint datapoint = new IncomingDataPoint(keyReplace, time.getTime(), new Double(subjectData.get(key).get(time)).toString(), tags);
					points.add(datapoint);
				}
				String json = gson.toJson(points);
				wr.write(json);
				wr.flush();
				wr.close();
				StringBuilder sb = new StringBuilder();  

				int HttpResult =conn.getResponseCode(); 

				if(HttpResult ==HttpURLConnection.HTTP_OK){

					BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));  

					String line = null;  

					while ((line = br.readLine()) != null) {  
						sb.append(line + "\n");  
					}  

					br.close();  

					System.out.println(""+sb.toString());  

				}else{
					System.out.println(conn.getResponseMessage());  
				}  
				conn.disconnect();
			}
		}

	}

	/**
	 * creates an {@link HSSFWorkbook} the specified OS filename.
	 */
	private static XSSFWorkbook readFile(String filename) throws IOException {
		return new XSSFWorkbook(new FileInputStream(filename));
	}

	/**
	 * @return the configFilename
	 */
	public String getConfigFilename() {
		return configFilename;
	}


	/**
	 * @return the openTSDBConfiguration
	 */
	public OpenTSDBConfiguration getOpenTSDBConfiguration() {
		return openTSDBConfiguration;
	}


	/**
	 * @param openTSDBConfiguration the openTSDBConfiguration to set
	 */
	public void setOpenTSDBConfiguration(OpenTSDBConfiguration openTSDBConfiguration) {
		this.openTSDBConfiguration = openTSDBConfiguration;
	}

}

