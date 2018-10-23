package com.mobikok.ssp.data.streaming.util;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKGroupTopicDirs;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaOffsetTool {

  private Logger LOG = new Logger(KafkaOffsetTool.class.getName(), new Date().getTime());

  private static KafkaOffsetTool instance;
  final int TIMEOUT = 100000;
  final int BUFFERSIZE = 64 * 1024;

  private KafkaOffsetTool() {
  }

  public static synchronized KafkaOffsetTool getInstance() {
    if (instance == null) {
      instance = new KafkaOffsetTool();
    }
    return instance;
  }

  public Map<TopicAndPartition, Long> getLastOffset(String brokerList, List<String> topics, String groupId) {
      while (true){
        try {
          return getLastOffset0(brokerList, topics, groupId);
        }catch (Throwable e) {
          LOG.error("Get kafka last offset fail, will try again!! exception:" , e);
          try {
            Thread.sleep(1000*10);
          } catch (Throwable ex) { }
        }
      }

  }

  public Map<TopicAndPartition, Long> getLastOffset0(String brokerList, List<String> topics,
      String groupId) {

    Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

    Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap = KafkaOffsetTool.getInstance().findLeader(brokerList, topics);

    for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap.entrySet()) {
      // get leader broker
      SimpleConsumer simpleConsumer = null;
      try {
        BrokerEndPoint leaderBroker = topicAndPartitionBrokerEntry.getValue();

        simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), TIMEOUT, BUFFERSIZE, groupId);

        long readOffset = getTopicAndPartitionLastOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey(), groupId);

        topicAndPartitionLongMap.put(topicAndPartitionBrokerEntry.getKey(), readOffset);
      }finally {
        if(simpleConsumer != null) {
          simpleConsumer.close();
        }
      }
    }

    return topicAndPartitionLongMap;

  }

  /**
   * 得到所有的 TopicAndPartition
   *
   * @param brokerList
   * @param topics
   * @return topicAndPartitions
   */
  private Map<TopicAndPartition, BrokerEndPoint> findLeader(String brokerList, List<String> topics) {
    // get broker's url array
    String[] brokerUrlArray = getBorkerUrlFromBrokerList(brokerList);
    // get broker's port map
    Map<String, Integer> brokerPortMap = getPortFromBrokerList(brokerList);

    // create array list of TopicAndPartition
    Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap = Maps.newHashMap();

    for (String broker : brokerUrlArray) {

      SimpleConsumer consumer = null;
      try {
        // new instance of simple Consumer
        consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT, BUFFERSIZE,"leaderLookup" + new Date().getTime());

        TopicMetadataRequest req = new TopicMetadataRequest(topics);

        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();

        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            TopicAndPartition topicAndPartition =
                new TopicAndPartition(item.topic(), part.partitionId());
            topicAndPartitionBrokerMap.put(topicAndPartition, part.leader());
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }
    return topicAndPartitionBrokerMap;
  }

  /**
   * get last offset
   * @param consumer
   * @param topicAndPartition
   * @param clientName
   * @return
   */
  private long getTopicAndPartitionLastOffset(SimpleConsumer consumer,
      TopicAndPartition topicAndPartition, String clientName) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
        kafka.api.OffsetRequest.LatestTime(), 1));

    OffsetRequest request = new OffsetRequest( requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);

    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: "
              + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
      return 0;
    }
    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
    return offsets[0];
  }
  /**
   * 得到所有的broker url
   *
   * @param brokerlist
   * @return
   */
  private String[] getBorkerUrlFromBrokerList(String brokerlist) {
    String[] brokers = brokerlist.split(",");
    for (int i = 0; i < brokers.length; i++) {
      brokers[i] = brokers[i].split(":")[0];
    }
    return brokers;
  }

  /**
   * 得到broker url 与 其port 的映射关系
   *
   * @param brokerlist
   * @return
   */
  private Map<String, Integer> getPortFromBrokerList(String brokerlist) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    String[] brokers = brokerlist.split(",");
    for (String item : brokers) {
      String[] itemArr = item.split(":");
      if (itemArr.length > 1) {
        map.put(itemArr[0], Integer.parseInt(itemArr[1]));
      }
    }
    return map;
  }

  public static void main(String[] args) {
//	  modifyOffset();
    List<String> topics = Lists.newArrayList();
    topics.add("topic_ad_fill");
//    topics.store("bugfix");
    Map<TopicAndPartition, Long> topicAndPartitionLongMap =
        KafkaOffsetTool.getInstance().getLastOffset("node14:6667", topics, "my.group.id");

    for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
     System.out.println(entry.getKey().topic() + "-"+ entry.getKey().partition() + ":" + entry.getValue());
    }
  }
  
  public static void modifyOffset() {
	  
	  ZkClient zkClient = new ZkClient("node14:2181/testkafka");	
	  ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs("dw_test_group1", "topic_ad_fill");
	  int children = zkClient.countChildren(topicDirs.consumerDir());
	  
	  
	  Map<TopicAndPartition, Long> fromOffsets  =  new HashMap<TopicAndPartition, Long>();  

	  for (int i =0; i < children  ; i++) {
		  String partitionOffset = zkClient.<String>readData(topicDirs.consumerOffsetDir() + "/"+i);
		  TopicAndPartition tp = new TopicAndPartition("topic_ad_fill", i);
	        fromOffsets .put(tp , Long.valueOf(partitionOffset));  //将不同 partition 对应的 offset 增加到 fromOffsets 中
	  }
	  
	  System.out.println(fromOffsets);
	  
	  
//	  List<String> topics = Lists.newArrayList();
//	    topics.store("topic_ad_fill");
//	    
//	  /** 以下 矫正 offset */
//	    // 得到Topic/partition 的lastOffsets
//	    Map<TopicAndPartition, Long> topicAndPartitionLongMap =
//	        KafkaOffsetTool.getInstance().getLastOffset("node14:9092",
//	        		topics, "my.group.id");
//
//	    // 遍历每个Topic.partition
//	    for (Map.Entry<TopicAndPartition, Long> topicAndPartitionLongEntry : fromOffsets.entrySet()) {
//	      // fromOffset > lastOffset时
//	      if (topicAndPartitionLongEntry.getValue() >
//	          topicAndPartitionLongMap.get(topicAndPartitionLongEntry.getKey())) {
//	         //矫正fromoffset为offset初始值0
//	        topicAndPartitionLongEntry.setValue(0L);
//	      }
//	    }
//	    /** 以上 矫正 offset */
  }
}