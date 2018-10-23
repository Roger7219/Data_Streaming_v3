package com.mobikok.ssp.data.streaming;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;


// java -cp  /root/kairenlo/data-streaming/ssp/overall/data-streaming.jar -Djava.ext.dirs=/root/kairenlo/data-streaming/data_lib com.mobikok.ssp.data.streaming.HBaseRegionMergeTest2
public class HBaseRegionMergeTest2 {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.master", "node16:16000");
//        conf.set("hbase.zookeeper.quorum", "node14,node17,node15");

        conf.set("hbase.master", "node106:16000");
        conf.set("hbase.zookeeper.quorum", "node106,node107,node108");
        System.out.println("start..........................................");
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            List<HRegionInfo> regions = admin.getTableRegions(TableName.valueOf("SSP_SEND_DWI_PHOENIX_20180105"));
            Collections.sort(regions, new Comparator<HRegionInfo>() {
                public int compare(HRegionInfo r1, HRegionInfo r2) {
                    return Bytes.compareTo(r1.getStartKey(), r2.getStartKey());
                }
            });

            HRegionInfo preRegion = null;
            for(HRegionInfo r: regions) {
                try {
                    int index = regions.indexOf(r);
                    if(index%2 == 0) {
                        preRegion = r;
                    } else {
                        System.out.println("admin.mergeRegions..........................................."+ preRegion + ", " + r);
                        admin.mergeRegions(preRegion.getEncodedNameAsBytes(), r.getEncodedNameAsBytes(), true);
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}