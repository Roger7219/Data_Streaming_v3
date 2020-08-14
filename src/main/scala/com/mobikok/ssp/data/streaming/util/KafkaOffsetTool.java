package com.mobikok.ssp.data.streaming.util;


import com.google.common.collect.Maps;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKGroupTopicDirs;
import org.I0Itec.zkclient.ZkClient;

import java.util.*;

public class KafkaOffsetTool {

  private Logger LOG;

  public KafkaOffsetTool(String loggerName){
    LOG = new Logger(loggerName, KafkaOffsetTool.class, System.currentTimeMillis());
  }

  static final int TIMEOUT_MS = 5*1000;
  static final int BUFFERSIZE = 64 * 1024;

  public Map<TopicAndPartition, Long> getLatestOffset(String brokerList, List<String> topics, String clientId) {
     return getOffsetRunAgainIfError(brokerList, topics, clientId, kafka.api.OffsetRequest.LatestTime());
  }
  public Map<TopicAndPartition, Long> getEarliestOffset(String brokerList, List<String> topics, String clientId) {
    return getOffsetRunAgainIfError(brokerList, topics, clientId, kafka.api.OffsetRequest.EarliestTime());
  }

  public Set<TopicAndPartition> getTopicPartitions(String brokerList, List<String> topics){
    return getOffsetRunAgainIfError(brokerList, topics, "for_get_TopicPartitions_clientId", kafka.api.OffsetRequest.EarliestTime()).keySet();
  }

  private Map<TopicAndPartition, Long> getOffsetRunAgainIfError(String brokerList, List<String> topics, String clientId, long whichTime) {
    while (true){
      try {
        return getOffset0(brokerList, topics, clientId, whichTime);
      }catch (Throwable e) {
        LOG.error("Get kafka last offset fail, will try again, brokerList: " + brokerList + ", topics: " + OM.toJOSN(topics) + ", clientId: " + clientId +", whichTime: " + whichTime, e);
        try {
          Thread.sleep(1000*10);
        } catch (Throwable ex) { }
      }
    }
  }

  //whichTime 要获取offset的时间,-1 最新，-2 最早
  private Map<TopicAndPartition, Long> getOffset0(String brokerList, List<String> topics, String clientId, long whichTime) {

    Map<TopicAndPartition, Long> topicAndPartitionLongMap = Maps.newHashMap();

    Map<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerMap = findLeader(brokerList, topics);

    for (Map.Entry<TopicAndPartition, BrokerEndPoint> topicAndPartitionBrokerEntry : topicAndPartitionBrokerMap.entrySet()) {
      // get leader broker
      SimpleConsumer simpleConsumer = null;
      try {
        BrokerEndPoint leaderBroker = topicAndPartitionBrokerEntry.getValue();

        simpleConsumer = new SimpleConsumer(
                leaderBroker.host(),
                leaderBroker.port(),
                TIMEOUT_MS,
                BUFFERSIZE,
                clientId);

        long readOffset = getTopicAndPartitionLastOffset(simpleConsumer, topicAndPartitionBrokerEntry.getKey(), clientId, whichTime);

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
        consumer = new SimpleConsumer(broker, brokerPortMap.get(broker), TIMEOUT_MS, BUFFERSIZE,"leaderLookup" + new Date().getTime());

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
        LOG.error("Kafka findLeader fail, broker: " + broker + ", brokerPortMap: " + OM.toJOSN(brokerPortMap), e);
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
  private long getTopicAndPartitionLastOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition, String clientName, long whichTime) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime/*kafka.api.OffsetRequest.LatestTime()*/, 1));

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

}
