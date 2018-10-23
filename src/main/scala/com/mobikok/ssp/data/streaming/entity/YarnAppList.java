package com.mobikok.ssp.data.streaming.entity;

public class YarnAppList {

	private String applicationId ;
	private String applicationName ;
	private String applicationType ;
//	private String user ;
//	private String queue ;
	private String applicationState ;
	private String applicationFinalStatus ;
//	private String progress ;
//	private String trackingUrl ;
	
	public YarnAppList(String applicationId, String applicationName,
			String applicationType, String applicationState,
			String applicationFinalStatus) {
		super();
		this.applicationId = applicationId;
		this.applicationName = applicationName;
		this.applicationType = applicationType;
		this.applicationState = applicationState;
		this.applicationFinalStatus = applicationFinalStatus;
	}
	
	public String getApplicationId() {
		return applicationId;
	}
	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}
	public String getApplicationName() {
		return applicationName;
	}
	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}
	public String getApplicationType() {
		return applicationType;
	}
	public void setApplicationType(String applicationType) {
		this.applicationType = applicationType;
	}
	public String getApplicationState() {
		return applicationState;
	}
	public void setApplicationState(String applicationState) {
		this.applicationState = applicationState;
	}
	public String getApplicationFinalStatus() {
		return applicationFinalStatus;
	}
	public void setApplicationFinalStatus(String applicationFinalStatus) {
		this.applicationFinalStatus = applicationFinalStatus;
	}

	@Override
	public String toString() {
		return "applicationId=" + applicationId
				+ " , applicationName=" + applicationName + ", applicationType="
				+ applicationType + ", applicationState=" + applicationState
				+ ", applicationFinalStatus=" + applicationFinalStatus;
	}
	
}
