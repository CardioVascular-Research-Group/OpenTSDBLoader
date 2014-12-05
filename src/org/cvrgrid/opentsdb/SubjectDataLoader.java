package org.cvrgrid.opentsdb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
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
	
	public SubjectDataLoader(String openTSDBUrl, String apiPut, String apiQuery, String awareSupportedParams,
				String idMatch, String idMatchSheet, String rootDir, String folderPath, String study){
		openTSDBConfiguration = new OpenTSDBConfiguration();
		openTSDBConfiguration.setOpenTSDBUrl(openTSDBUrl);
		openTSDBConfiguration.setApiPut(apiPut);
		openTSDBConfiguration.setApiQuery(apiQuery);
		openTSDBConfiguration.setAwareSupportedParams(awareSupportedParams);
		openTSDBConfiguration.setIdMatch(idMatch);
		openTSDBConfiguration.setIdMatchSheet(idMatchSheet);
		openTSDBConfiguration.setRootDir(rootDir);
		openTSDBConfiguration.setFolderPath(folderPath);
		openTSDBConfiguration.setStudyString(study);
	}
	
	private ArrayList<String> extractXSData(XSSFWorkbook wb){
		
		ArrayList<String> subjectHashes = new ArrayList<String>();

		try {
//			wb = readFile(openTSDBConfiguration.getIdMatch());
			wb = new XSSFWorkbook(new FileInputStream(openTSDBConfiguration.getIdMatch()));
			XSSFSheet sheet = wb.getSheet(openTSDBConfiguration.getIdMatchSheet());
			for (int r = 1; r < sheet.getLastRowNum()+1; r++) {
				XSSFRow row = sheet.getRow(r);
				if (row == null) {
					continue;
				}
				subjectHashes.add(row.getCell(2).getStringCellValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return subjectHashes;
	}
	
	private void formatArray(String[] variableAndUnits, HashMap<String,String> tags){
		
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
	}
	
	private HttpURLConnection openHTTPConnection(){
		String urlString = openTSDBConfiguration.getOpenTSDBUrl() + openTSDBConfiguration.getApiPut();
		URL url = null;
		HttpURLConnection conn = null;
		try {
			url = new URL(urlString);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-type", "application/json");
			conn.setDoOutput(true);
			conn.setDoInput(true);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	private void readHTTPConnection(HttpURLConnection conn){
		StringBuilder sb = new StringBuilder();  
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));
			String line = null;  
			while ((line = br.readLine()) != null) {  
				sb.append(line + "\n");  
			}  
			br.close(); 
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  
		System.out.println(""+sb.toString());  
	}
	
	public void uploadFile(InputStream fileStream){
		
		Gson gson = new Gson();
		int subjectCount = 0;
		XSSFWorkbook wb = null;
		ArrayList<String> subjectHashes = extractXSData(wb);
		
		for (String subjectHash : subjectHashes) {
			HashMap<String,Hashtable<Date, Double>> subjectData = new HashMap<String,Hashtable<Date, Double>>();
			Hashtable<Date, Double> timeSeries = new Hashtable<Date, Double>();
			HashMap<String,String> tags = new HashMap<String,String>();
			tags.put("subjectHash", subjectHash);
			SimpleDateFormat fromExcel = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String subjectId = "";
			TreeSet<String> sortedKeys = null;
			try {
				wb = new XSSFWorkbook(fileStream);
//				wb = readFile(openTSDBConfiguration.getFolderPath() + subjectHash + ".xlsx");
//				wb = new XSSFWorkbook(new FileInputStream(openTSDBConfiguration.getFolderPath() + subjectHash + ".xlsx"));
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
				sortedKeys = new TreeSet<String>(keys);
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
			
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			tags.put("subjectId", subjectId);
			
			try{
				for (String key : sortedKeys) {
					
					HttpURLConnection conn = openHTTPConnection();
					OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
					String[] variableAndUnits = key.split("\\(");
					String keyReplace = variableAndUnits[0];
					keyReplace = keyReplace.replaceAll(" ", "");
					if (keyReplace.equalsIgnoreCase("PulseOximetryPeripheralHeart"))
						keyReplace += "Rate";
					
					formatArray(variableAndUnits, tags);
					
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
	
	
					int HttpResult = conn.getResponseCode(); 
	
					if(HttpResult == HttpURLConnection.HTTP_OK){
						readHTTPConnection(conn);
					}else{
						System.out.println(conn.getResponseMessage());  
					}  
					conn.disconnect();
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}

