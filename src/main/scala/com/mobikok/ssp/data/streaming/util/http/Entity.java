package com.mobikok.ssp.data.streaming.util.http;

/**
 * Created by Administrator on 2017/7/12.
 */
public class Entity {

    public static String CONTENT_TYPE_JSON = "application/json";
    public static String CONTENT_TYPE_FORM = "application/x-www-form-urlencoded";

    public static String CONTENT_TYPE_TEXT = "text/html";

    private Str str;
    private JSON json;
    private Form form;

    private Type currType;

    public Entity setStr(String str, String contentType){
        checkType(Type.STR);
        this.currType = Type.STR;
        this.str =  new Str(str, contentType);
        return this;
    }

    public Entity setJson(Object bean){
        checkType(Type.JSON);
        this.currType = Type.JSON;
        json = new JSON(bean);

        return this;
    }
    public Entity addForm(String name, String value){
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
            case STR:
                return str.build();
            case FORM:
                return form.build();
            case JSON:
                return json.build();
            default:
                return "";
        }
    }
    private Type getCurrType() {
        return currType;
    }

    public String contentType(){
        switch (getCurrType()) {
            case STR:
                return str.getContentType();
            case FORM:
                return CONTENT_TYPE_FORM;
            case JSON:
                return CONTENT_TYPE_JSON;
            default:
                return CONTENT_TYPE_TEXT;
        }
    }

    private void checkType(Type type){
        if(currType != type && currType != null) {
            throw new RuntimeException("Entity content type already is " + currType +", Cannot reset another type data");
        }
    }
    public enum Type{
        STR,JSON,FORM
    }
}