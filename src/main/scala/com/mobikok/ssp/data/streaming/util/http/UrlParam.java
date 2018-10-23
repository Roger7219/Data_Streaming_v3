package com.mobikok.ssp.data.streaming.util.http;


public class UrlParam {

	private Form form;
	
	private Type currType;
	
	public UrlParam addParam(String name, String value){
		checkType(Type.FORM);
		this.currType = Type.FORM;
		if(form == null) {
			form = new Form();
		}
		form.add(name, value);
		return this;
	}
	
	public String build(){
		switch (getCurrType()) {
		case FORM:
			return form.build();
		default:
			return "";
		} 
	}
	public Type getCurrType() {
		return currType;
	}

	private void checkType(Type type){
		if(currType != type && currType != null) {
			throw new RuntimeException("Entity content type already is " + currType +", Cannot reset another type data");
		}
	}
	enum Type{
		FORM
	}
	public boolean isEmpty() {
		return form == null;
	}
}
