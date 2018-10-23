package com.mobikok.ssp.data.streaming.bq;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Hello world!
 *
 */
/**
 * 
 * #standardSQL insert into my_new_dataset.my_new_table(f1) select text from
 * `bigquery-public-data.hacker_news.comments`
 * 
 */
public class UploadCampaginTest {

	static String CONNECTION_URL = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=dogwood-seeker-182806;OAuthType=0;OAuthServiceAcctEmail=kairenlo@msn.cn;OAuthPvtKeyPath=C:\\Users\\Administrator\\Desktop\\key.json;";

	public static void main(String... args) throws Exception {
		System.setProperty("http.proxySet", "true");    //设置使用网络代理
		System.setProperty("http.proxyHost", "192.168.1.6");  //设置代理服务器地址
		System.setProperty("http.proxyPort", "808");    //设置代理服务器端口号

		System.setProperty("proxySet", "true");
		System.setProperty("socksProxyHost", "192.168.1.6");
		System.setProperty("socksProxyPort", "1080");
//		Path csvPath = Paths.get(new File("C:/Users/Administrator/Desktop/part-00001-20b8b73a-4936-44bb-a32f-8405bdb781e5.csv.gz").toURI());
		Path csvPath = Paths.get(new File("C:/Users/Administrator/Desktop/p2.gz").toURI());

		

		BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("dogwood-seeker-182806").setCredentials(ServiceAccountCredentials.fromStream(
				new FileInputStream("C:/Users/Administrator/Desktop/key.json") )).build().getService();
		 
//		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of("my_new_dataset", "upload_campagin_table");
		WriteChannelConfiguration writeChannelConfiguration =
		    WriteChannelConfiguration
		    	.newBuilder(tableId)
//		    	.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
//		    	.setAutodetect(true)
//		    	.setSchema(
//		    			Schema.newBuilder()
//		    			.addField(Field.of("publisherid", Type.integer()))
//		    			.addField(Field.of("appid", Type.integer()))
//		    			.addField(Field.of("countryid", Type.integer()))
//		    			.addField(Field.of("carrierid", Type.integer()))
//		    			.addField(Field.of("adtype", Type.integer()))
//		    			.addField(Field.of("campaignid", Type.integer()))
//		    			.addField(Field.of("offerid", Type.integer()))
//		    			.addField(Field.of("imageid", Type.integer()))
//		    			.addField(Field.of("affsub", Type.string()))
//		    			.addField(Field.of("requestcount", Type.integer()))
//		    			.addField(Field.of("sendcount", Type.integer()))
//		    			.addField(Field.of("showcount", Type.integer()))
//		    			.addField(Field.of("clickcount", Type.integer()))
//		    			.addField(Field.of("feereportcount", Type.integer()))
//		    			.addField(Field.of("feesendcount", Type.integer()))
//		    			.addField(Field.of("feereportprice", Type.floatingPoint()))
//		    			.addField(Field.of("feesendprice", Type.floatingPoint()))
//		    			.addField(Field.of("cpcbidprice", Type.floatingPoint()))
//		    			.addField(Field.of("cpmbidprice", Type.floatingPoint()))
//		    			.addField(Field.of("conversion", Type.integer()))
//		    			.addField(Field.of("allconversion", Type.integer()))
//		    			.addField(Field.of("revenue", Type.floatingPoint()))
//		    			.addField(Field.of("realrevenue", Type.floatingPoint()))
//		    			.addField(Field.of("l_time", Type.string()))
//		    			.addField(Field.of("b_date", Type.string()))
//		    			
//		    			.addField(Field.of("publisheramid", Type.integer()))
//		    			.addField(Field.of("publisheramname", Type.string()))
//		    			.addField(Field.of("advertiseramid", Type.integer()))
//		    			.addField(Field.of("advertiseramname", Type.string()))
//		    			.addField(Field.of("appmodeid", Type.integer()))
//		    			.addField(Field.of("appmodename", Type.string()))
//		    			.addField(Field.of("adcategory1id", Type.integer()))
//		    			.addField(Field.of("adcategory1name", Type.string()))
//		    			.addField(Field.of("campaignname", Type.string()))
//		    			.addField(Field.of("adverid", Type.integer()))
//		    			.addField(Field.of("advername", Type.string()))
//		    			.addField(Field.of("offeroptstatus", Type.integer()))
//		    			.addField(Field.of("offername", Type.string()))
//		    			.addField(Field.of("publishername", Type.string()))
//		    			.addField(Field.of("appname", Type.string()))
//		    			.addField(Field.of("countryname", Type.string()))
//		    			.addField(Field.of("carriername", Type.string()))
//		    			.addField(Field.of("adtypeid", Type.integer()))
//		    			.addField(Field.of("adtypename", Type.string()))
//		    			.addField(Field.of("versionid", Type.integer()))
//		    			.addField(Field.of("versionname", Type.string()))
//		    			.addField(Field.of("publisherproxyid", Type.integer()))
//		    			
//		    			.build() 
//		    			 
//		    			)
		        .setFormatOptions(FormatOptions.csv())
		        .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
		        .build();
		TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
		// Write data to writer
		try  {
			OutputStream stream = Channels.newOutputStream(writer);
		  	Files.copy(csvPath, stream);
		}catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println();
		// Get load job
		Job job = writer.getJob();
		job = job.waitFor();
		JobStatistics stats = job.getStatistics();
		System.out.println(stats);
		 
	}
}
