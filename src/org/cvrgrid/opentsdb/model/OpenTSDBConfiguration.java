package org.cvrgrid.opentsdb.model;

import java.io.Serializable;

public class OpenTSDBConfiguration implements Serializable {

	private static final long serialVersionUID = 6721093052749142738L;
	private String openTSDBUrl;
	private String apiPut;
	private String apiQuery;
	private String awareSupportedParams;
	private String idMatch;
	private String idMatchSheet;
	private String rootDir;
	private String folderPath;
	private String studyString;

	public OpenTSDBConfiguration() {

	}

	/**
	 * @return the openTSDBUrl
	 */
	public String getOpenTSDBUrl() {
		return openTSDBUrl;
	}

	/**
	 * @param openTSDBUrl the openTSDBUrl to set
	 */
	public void setOpenTSDBUrl(String openTSDBUrl) {
		this.openTSDBUrl = openTSDBUrl;
	}

	/**
	 * @return the apiPut
	 */
	public String getApiPut() {
		return apiPut;
	}

	/**
	 * @param apiPut the apiPut to set
	 */
	public void setApiPut(String apiPut) {
		this.apiPut = apiPut;
	}

	/**
	 * @return the apiQuery
	 */
	public String getApiQuery() {
		return apiQuery;
	}

	/**
	 * @param apiQuery the apiQuery to set
	 */
	public void setApiQuery(String apiQuery) {
		this.apiQuery = apiQuery;
	}

	/**
	 * @return the awareSupportedParams
	 */
	public String getAwareSupportedParams() {
		return awareSupportedParams;
	}

	/**
	 * @param awareSupportedParams the awareSupportedParams to set
	 */
	public void setAwareSupportedParams(String awareSupportedParams) {
		this.awareSupportedParams = awareSupportedParams;
	}

	/**
	 * @return the idMatch
	 */
	public String getIdMatch() {
		return idMatch;
	}

	/**
	 * @param idMatch the idMatch to set
	 */
	public void setIdMatch(String idMatch) {
		this.idMatch = idMatch;
	}

	/**
	 * @return the idMatchSheet
	 */
	public String getIdMatchSheet() {
		return idMatchSheet;
	}

	/**
	 * @param idMatchSheet the idMatchSheet to set
	 */
	public void setIdMatchSheet(String idMatchSheet) {
		this.idMatchSheet = idMatchSheet;
	}

	/**
	 * @return the rootDir
	 */
	public String getRootDir() {
		return rootDir;
	}

	/**
	 * @param rootDir the rootDir to set
	 */
	public void setRootDir(String rootDir) {
		this.rootDir = rootDir;
	}

	/**
	 * @return the folderPath
	 */
	public String getFolderPath() {
		return folderPath;
	}

	/**
	 * @param folderPath the folderPath to set
	 */
	public void setFolderPath(String folderPath) {
		this.folderPath = folderPath;
	}

	/**
	 * @return the studyString
	 */
	public String getStudyString() {
		return studyString;
	}

	/**
	 * @param studyString the studyString to set
	 */
	public void setStudyString(String studyString) {
		this.studyString = studyString;
	}

}
