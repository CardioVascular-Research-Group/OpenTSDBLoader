package org.cvrgrid.opentsdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.cvrgrid.opentsdb.model.HL7Measurements;
import org.cvrgrid.opentsdb.model.IncomingDataPoint;
import org.cvrgrid.opentsdb.model.Metric;
import org.cvrgrid.opentsdb.model.OpenTSDBConfiguration;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v23.group.ORU_R01_OBSERVATION;
import ca.uhn.hl7v2.model.v23.message.ORU_R01;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import ca.uhn.hl7v2.util.Terser;

public class Hl7ToOpenTSDB { 
	
	private String configFilename = "/resources/server.properties";
	private OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();

	/**
	 * Constructor for this code intended to set all the variables based upon the properties file.
	 */
	public Hl7ToOpenTSDB(){		 

		try {

			OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();
			Properties serverProperties = new Properties();
			InputStream stream = Hl7ToOpenTSDB.class.getResourceAsStream(this.getConfigFilename());
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

		Hl7ToOpenTSDB hl7ToOpenTSDB = new Hl7ToOpenTSDB();
		OpenTSDBConfiguration openTSDBConfiguration = hl7ToOpenTSDB.getOpenTSDBConfiguration();
		HL7Measurements hl7Measurements = new HL7Measurements();
		HashMap<String,String> measurementNames = hl7Measurements.getMeasurementNames();
		HashMap<String,String> subjectHashAndFirstTime = new HashMap<String,String>();
		HashMap<String,String> hashIdMap = new HashMap<String,String>();
		XSSFWorkbook wb = readFile(openTSDBConfiguration.getAwareSupportedParams());
		XSSFSheet sheet = wb.getSheetAt(0);
		for (int r = 1; r < 280; r++) {
			XSSFRow row = sheet.getRow(r);
			if (row == null) {
				continue;
			}
			measurementNames.put(row.getCell(2).getStringCellValue(), row.getCell(1).getStringCellValue());
		}
		wb = readFile(openTSDBConfiguration.getIdMatch());
		sheet = wb.getSheet(openTSDBConfiguration.getIdMatchSheet());
		for (int r = 1; r < sheet.getLastRowNum()+1; r++) {
			XSSFRow row = sheet.getRow(r);
			if (row == null) {
				continue;
			}
			subjectHashAndFirstTime.put(row.getCell(2).getStringCellValue(),row.getCell(8).getStringCellValue());
		}
		String urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiPut();
		SimpleDateFormat fromHL7 = new SimpleDateFormat("yyyyMMddHHmmss");
		SimpleDateFormat fromExcel = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Gson gson = new Gson();
		int subjectCount = 0;
		Set<String> keys = subjectHashAndFirstTime.keySet();
		TreeSet<String> sortedKeys = new TreeSet<String>(keys);
		Date reformattedTime;
		HttpURLConnection conn = null;
		long startTime, endTime;

		for (String subjectHash : sortedKeys) {
			urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiQuery();
			reformattedTime = fromExcel.parse(subjectHashAndFirstTime.get(subjectHash));
			startTime = reformattedTime.getTime();
			endTime = startTime + 1;
			urlString += "?start=" + startTime + "&end=" + endTime + "&m=sum:HeartRate{subjectHash=" + subjectHash + "}";
			try {
				conn = buildConnection(urlString, "GET");
				if (conn.getResponseCode() == 200) {
					BufferedReader br = new BufferedReader(new InputStreamReader(
							(conn.getInputStream())));

					String output = "";
					StringBuffer sb = new StringBuffer();
					while ((output = br.readLine()) != null) {

						output = output.trim();
						sb.append(output + "\n");

					}
					Type metricType = new TypeToken<ArrayList<Metric>>() {}.getType();
					ArrayList<Metric> queryResponse = gson.fromJson(sb.toString(), metricType);
					for (Metric metric : queryResponse){
						hashIdMap.put(metric.getTags().get("subjectHash"), metric.getTags().get("subjectId"));
						if (new Integer(metric.getTags().get("subjectId").substring(4)).intValue() > subjectCount)
							subjectCount = new Integer(metric.getTags().get("subjectId").substring(4)).intValue();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			conn = null;
		}
		System.out.println("Highest existing subject id:" + subjectCount);
		String rootDir = openTSDBConfiguration.getRootDir();
		ArrayList<String> messageFiles = new ArrayList<String>();
		File rootDirContents = new File (rootDir);
		getDirectoryContents(rootDirContents, messageFiles);
		for (String filePath : messageFiles) {
			System.out.println("     file:" + filePath);
			FileReader reader = new FileReader(filePath);
			//			FileReader reader = new FileReader(rootDir + "xfer Aug 14 2014\\his_eg_outbound_data.20140804070002_.msg");

			Hl7InputStreamMessageIterator iter = new Hl7InputStreamMessageIterator(reader);

			while (iter.hasNext()) {
				HashMap<String,String> tags = new HashMap<String,String>();
				Message next = iter.next();
				ORU_R01 oru = new ORU_R01();
				oru.parse(next.encode());
				String firstName = Terser.get(oru.getRESPONSE().getPATIENT().getPID(), 5, 0, 2, 1);
				String lastName = Terser.get(oru.getRESPONSE().getPATIENT().getPID(), 5, 0, 1, 1);
				String birthDateTime = Terser.get(oru.getRESPONSE().getPATIENT().getPID(), 7, 0, 1, 1);
				String gender = Terser.get(oru.getRESPONSE().getPATIENT().getPID(), 8, 0, 1, 1);
				String birthplace = Terser.get(oru.getRESPONSE().getPATIENT().getPID(), 23, 0, 1, 1);
				String concatenation = firstName+lastName+birthDateTime+gender+birthplace;
				String subjectHash = null; 
				try {
					MessageDigest sha = MessageDigest.getInstance("SHA-256");
					byte[] result =  sha.digest(concatenation.getBytes());
					subjectHash = hexEncode(result);
					subjectHash = escapeHtml(subjectHash);
				}
				catch ( NoSuchAlgorithmException ex ) {
					System.err.println(ex);
				}
				if (!keys.contains(subjectHash))
					continue;
				tags.put("subjectHash", subjectHash);
				String subjectId;
				if (hashIdMap.get(subjectHash) != null) {
					subjectId = hashIdMap.get(subjectHash);
				} else {
					subjectId = openTSDBConfiguration.getStudyString();
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
					hashIdMap.put(subjectHash, subjectId);
				}
				tags.put("subjectId", subjectId);
				System.out.println(subjectId);
				String time = Terser.get(oru.getRESPONSE().getORDER_OBSERVATION().getOBR(), 7, 0, 1, 1);
				reformattedTime = fromHL7.parse(time);
				List<ORU_R01_OBSERVATION> observations = oru.getRESPONSE().getORDER_OBSERVATION().getOBSERVATIONAll();
				for (ORU_R01_OBSERVATION observation : observations) {
					String seriesName = Terser.get(observation.getOBX(), 3, 0, 1, 1);
					if (measurementNames.get(seriesName) != null) {
						seriesName = measurementNames.get(seriesName);
					} else {
						seriesName = seriesName.replaceFirst("\\d", "#");
						seriesName = measurementNames.get(seriesName);
					}
					seriesName = seriesName.trim();
					seriesName = seriesName.replaceAll(" ", "");
					urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiPut();
					conn = buildConnection(urlString, "POST");
					OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
					try {
						String measurementValue = Terser.get(observation.getOBX(), 5, 0, 1, 1);
						String units = Terser.get(observation.getOBX(), 6, 0, 1, 1);
						if (units != null) {
							units = units.replaceAll("/", "per");
							units = units.replaceAll("%", "percent");
							units = units.replaceAll("#", "count");
							units = units.replaceAll("cel", "Celsius");
							units = units.replaceAll("mm\\(hg\\)", "mmHg");
							tags.put("units", units);
						}
						IncomingDataPoint datapoint = new IncomingDataPoint(seriesName, reformattedTime.getTime(), measurementValue, tags);
						String json = gson.toJson(datapoint);
						//System.out.println(json);
						wr.write(json);
						wr.flush();
						wr.close();
						StringBuilder sb = new StringBuilder();  

						int HttpResult = conn.getResponseCode(); 

						if(HttpResult == HttpURLConnection.HTTP_OK){

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
					} catch (IOException e) {
						e.printStackTrace();
					} 
				}
			}
		}
	}


	private static HttpURLConnection buildConnection(String urlString, String method) {
		URL url;
		try {
			url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(method);
			connection.setRequestProperty("Accept", "application/json");
			connection.setRequestProperty("Content-type", "application/json");
			if (method.equalsIgnoreCase("post")) {
				connection.setDoOutput(true);
			} else {
				connection.setDoInput(true);
			}
			return connection;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}


	/**
	 * The byte[] returned by MessageDigest does not have a nice
	 * textual representation, so some form of encoding is usually performed.
	 *
	 * This implementation follows the example of David Flanagan's book
	 * "Java In A Nutshell", and converts a byte array into a String
	 * of hex characters.
	 *
	 * Another popular alternative is to use a "Base64" encoding.
	 **/
	static private String hexEncode( byte[] aInput){
		StringBuilder result = new StringBuilder();
		char[] digits = {'0', '1', '2', '3', '4','5','6','7','8','9','a','b','c','d','e','f'};
		for ( int idx = 0; idx < aInput.length; ++idx) {
			byte b = aInput[idx];
			result.append( digits[ (b&0xf0) >> 4 ] );
			result.append( digits[ b&0x0f] );
		}
		return result.toString();
	} 

	/**
	 * Escape an html string. Escaping data received from the client helps to
	 * prevent cross-site script vulnerabilities.
	 * 
	 * @param html the html string to escape
	 * @return the escaped string
	 **/

	static private String escapeHtml(String html) {
		if (html == null) {
			return null;
		}
		return html.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
	}

	private static ArrayList<String> getDirectoryContents(File dir, ArrayList<String> messageFiles) {
		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					messageFiles = getDirectoryContents(file, messageFiles);
				} else {
					if((file.getCanonicalPath().endsWith(".txt")) || (file.getCanonicalPath().endsWith(".msg")))
						messageFiles.add(file.getCanonicalPath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return messageFiles;
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