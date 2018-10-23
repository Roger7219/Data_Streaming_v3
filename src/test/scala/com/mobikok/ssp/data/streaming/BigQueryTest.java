package com.mobikok.ssp.data.streaming;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static javolution.testing.TestContext.*;

/**
 * Created by Administrator on 2017/12/27.
 */
public class BigQueryTest {

    //https://github.com/GoogleCloudPlatform/google-cloud-java/blob/master/google-cloud-bigquery/src/test/java/com/google/cloud/bigquery/it/ITBigQueryTest.java
    public static String DATASET = "";
    static Schema TABLE_SCHEMA =  null;

    static BigQuery bigquery = null;

    public static void init(){
        try {
            System.setProperty("http.proxySet", "true"); // 设置使用网络代理
            System.setProperty("http.proxyHost", "192.168.1.6"); // 设置代理服务器地址
            System.setProperty("http.proxyPort", "808"); // 设置代理服务器端口号

            System.setProperty("proxySet", "true");
            System.setProperty("socksProxyHost", "192.168.1.6");
            System.setProperty("socksProxyPort", "1080");

            bigquery = BigQueryOptions.newBuilder().setProjectId("dogwood-seeker-182806").setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("key.json"))).build().getService();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        init();

    }

    public void testDelete(){

    }

    public static void testCopyJob() throws InterruptedException, TimeoutException {
        String sourceTableName = "test_copy_job_source_table";
        String destinationTableName = "test_copy_job_destination_table";

        TableId sourceTable = TableId.of(DATASET, sourceTableName);
        StandardTableDefinition tableDefinition = StandardTableDefinition.of(TABLE_SCHEMA);
        TableInfo tableInfo = TableInfo.of(sourceTable, tableDefinition);

        Table createdTable = bigquery.create(tableInfo);

        assertNotNull(createdTable);
        assertEquals(DATASET, createdTable.getTableId().getDataset());
        assertEquals(sourceTableName, createdTable.getTableId().getTable());

        TableId destinationTable = TableId.of(DATASET, destinationTableName);

        CopyJobConfiguration configuration = CopyJobConfiguration
                .newBuilder(destinationTable, sourceTable).setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE).build();

        Job remoteJob = bigquery.create(JobInfo.of(configuration));
        remoteJob = remoteJob.waitFor();
        assertNull(remoteJob.getStatus().getError());
        Table remoteTable = bigquery.getTable(DATASET, destinationTableName);
        assertNotNull(remoteTable);
        assertEquals(destinationTable.getDataset(), remoteTable.getTableId().getDataset());
        assertEquals(destinationTableName, remoteTable.getTableId().getTable());
        assertEquals(TABLE_SCHEMA, remoteTable.getDefinition().getSchema());
        assertTrue(createdTable.delete());
        assertTrue(remoteTable.delete());
    }

}
