package org.cvrgrid.opentsdb;

import java.io.BufferedReader;
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
import java.util.Set;
import java.util.TreeSet;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.cvrgrid.opentsdb.model.IncomingDataPoint;
import org.cvrgrid.opentsdb.model.OpenTSDBConfiguration;

import com.google.gson.Gson;

import edu.jhu.cvrg.filestore.main.FileStorer;


public class SubjectDataLoader {

	private OpenTSDBConfiguration openTSDBConfiguration = new OpenTSDBConfiguration();
	private FileStorer fileStorer;
	
	public SubjectDataLoader(String openTSDBUrl, String apiPut, String apiQuery, String awareSupportedParams,
				String idMatch, String idMatchSheet, String rootDir, String folderPath, String study, FileStorer fileStorer){
		openTSDBConfiguration.setOpenTSDBUrl(openTSDBUrl);
		openTSDBConfiguration.setApiPut(apiPut);
		openTSDBConfiguration.setApiQuery(apiQuery);
		openTSDBConfiguration.setAwareSupportedParams(awareSupportedParams);
		openTSDBConfiguration.setIdMatch(idMatch);
		openTSDBConfiguration.setIdMatchSheet(idMatchSheet);
		openTSDBConfiguration.setRootDir(rootDir);
		openTSDBConfiguration.setFolderPath(folderPath);
		openTSDBConfiguration.setStudyString(study);
		this.fileStorer = fileStorer;
	}
	
	private ArrayList<String> extractXSData(XSSFSheet sheet){

		ArrayList<String> subjectHashes = new ArrayList<String>();
			for (int r = 1; r < sheet.getLastRowNum()+1; r++) {
				XSSFRow row = sheet.getRow(r);
				if (row == null) {
					continue;
				}
				subjectHashes.add(row.getCell(2).getStringCellValue());
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
	
	private XSSFWorkbook getWorkbook(InputStream inputStream){
		XSSFWorkbook matchWorkbook = null;
		try {
			matchWorkbook = new XSSFWorkbook(inputStream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return matchWorkbook;
	}

	public void uploadFile(String fileName){
		
		Gson gson = new Gson();
		int subjectCount = 0;
		XSSFWorkbook matchWorkbook = getWorkbook(fileStorer.retrieveFile(fileName));
		XSSFSheet matchSheet = matchWorkbook.getSheet(openTSDBConfiguration.getIdMatchSheet());
		ArrayList<String> subjectHashes = extractXSData(matchSheet);

		for (String subjectHash : subjectHashes) {
			XSSFWorkbook subjectWorkbook = getWorkbook(fileStorer.retrieveFile(subjectHash + ".xlsx"));
			HashMap<String,Hashtable<Date, Double>> subjectData = new HashMap<String,Hashtable<Date, Double>>();
			Hashtable<Date, Double> timeSeries = new Hashtable<Date, Double>();
			HashMap<String,String> tags = new HashMap<String,String>();
			tags.put("subjectHash", subjectHash);
			SimpleDateFormat fromExcel = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String subjectId = "";
			TreeSet<String> sortedKeys = null;
			try {
				for (int i = 0; i < subjectWorkbook.getNumberOfSheets(); i++) {
					XSSFSheet sheetIn = subjectWorkbook.getSheetAt(i);
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
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			tags.put("subjectId", subjectId);
			
			try{
				if(sortedKeys == null){
					break;
				}
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

