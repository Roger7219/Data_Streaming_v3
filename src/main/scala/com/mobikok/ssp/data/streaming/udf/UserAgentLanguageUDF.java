package com.mobikok.ssp.data.streaming.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 2017/12/20.
 */
public class UserAgentLanguageUDF extends UDF implements Serializable {
///Mozilla/5.0 (Linux; Android 5.1; Micromax Q413 Build/LMY47D; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/58.0.3029.83 Mobile Safari/537.36
    public String evaluate(Text str) {
        try {
            String c = str.toString().toLowerCase();
            for(String lang :langs) {
                if(c.contains(lang)) {
                    return lang;
                }
            }
            return "Unknown";
        } catch (Throwable e) {
            return "Unknown";//ExceptionUtils.getFullStackTrace(e);
        }
    }

    private List<String> langs = new ArrayList<String>(){{
        add("zh-cn");
        add("zh-hk");
        add("zh-mo");
        add("zh-sg");
        add("zh-tw");
        add("zh-chs");
        add("zh-cht");

        add("en-au");
        add("en-bz");
        add("en-ca");
        add("en-cb");
        add("en-ie");
        add("en-jm");
        add("en-nz");
        add("en-ph");
        add("en-za");
        add("en-tt");
        add("en-gb");
        add("en-us");
        add("en-zw");

        add("af-za");
        add("sq-al");
        add("ar-dz");
        add("ar-bh");
        add("ar-eg");
        add("ar-iq");
        add("ar-jo");
        add("ar-kw");
        add("ar-lb");
        add("ar-ly");
        add("ar-ma");
        add("ar-om");
        add("ar-qa");
        add("ar-sa");
        add("ar-sy");
        add("ar-tn");
        add("ar-ae");
        add("ar-ye");
        add("hy-am");
        add("cy-az-az");
        add("lt-az-az");
        add("eu-es");
        add("be-by");
        add("bg-bg");
        add("ca-es");

        add("hr-hr");
        add("cs-cz");
        add("da-dk");
        add("div-mv");
        add("nl-be");
        add("nl-nl");

        add("et-ee");
        add("fo-fo");
        add("fa-ir");
        add("fi-fi");
        add("fr-be");
        add("fr-ca");
        add("fr-fr");
        add("fr-lu");
        add("fr-mc");
        add("fr-ch");
        add("gl-es");
        add("ka-ge");
        add("de-at");
        add("de-de");
        add("de-li");
        add("de-lu");
        add("de-ch");
        add("el-gr");
        add("gu-in");
        add("he-il");
        add("hi-in");
        add("hu-hu");
        add("is-is");
        add("id-id");
        add("it-it");
        add("it-ch");
        add("ja-jp");
        add("kn-in");
        add("kk-kz");
        add("kok-in");
        add("ko-kr");
        add("ky-kz");
        add("lv-lv");
        add("lt-lt");
        add("mk-mk");
        add("ms-bn");
        add("ms-my");
        add("mr-in");
        add("mn-mn");
        add("nb-no");
        add("nn-no");
        add("pl-pl");
        add("pt-br");
        add("pt-pt");
        add("pa-in");
        add("ro-ro");
        add("ru-ru");
        add("sa-in");
        add("cy-sr-sp");
        add("lt-sr-sp");
        add("sk-sk");
        add("sl-si");
        add("es-ar");
        add("es-bo");
        add("es-cl");
        add("es-co");
        add("es-cr");
        add("es-do");
        add("es-ec");
        add("es-sv");
        add("es-gt");
        add("es-hn");
        add("es-mx");
        add("es-ni");
        add("es-pa");
        add("es-py");
        add("es-pe");
        add("es-pr");
        add("es-es");
        add("es-uy");
        add("es-ve");
        add("sw-ke");
        add("sv-fi");
        add("sv-se");
        add("syr-sy");
        add("ta-in");
        add("tt-ru");
        add("te-in");
        add("th-th");
        add("tr-tr");
        add("uk-ua");
        add("ur-pk");
        add("cy-uz-uz");
        add("lt-uz-uz");
        add("vi-vn");
    }};



}
