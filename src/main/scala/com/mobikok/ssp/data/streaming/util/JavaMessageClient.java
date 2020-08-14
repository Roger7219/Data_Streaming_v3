package com.mobikok.ssp.data.streaming.util;


import com.fasterxml.jackson.core.type.TypeReference;
import com.mobikok.message.*;
import com.mobikok.message.client.MessageClientApi;
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart;

import java.util.*;

/**
 * 这是Java版的MC实现，针对MessageClient的封装，主要是针对流统计相关的分区（b_time,b_date和b_date）消息的封装
 * 如果用Scala语言，最好用Scala版的 MC类
 */
public class JavaMessageClient {

    public static void pullAndSortByLTimeDescHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClientApi, consumer, "l_time", callback, topics);
    }

    public static void pullAndSortByBDateDescHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClientApi, consumer, "b_date",callback, topics);
    }

    public static void pullAndSortByBTimeDescHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClientApi, consumer, "b_time",callback, topics);
    }

    public static void pullAndSortByBTimeDescHivePartitionParts(MessageClientApi messageClientApi, String consumer, HivePartitionPartsPartialCommitCallback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClientApi, consumer, "b_time", callback, topics);
    }



//    private static HivePartitionPart[][] toHivePartitionParts(Resp<List<Message>> resp, String partitionFieldName){
//
//    }

    private static List<HivePartitionPart> toHivePartitionPartsDesc(Resp<List<Message>> resp, String partitionFieldName){
        List<HivePartitionPart> result = new ArrayList<HivePartitionPart>();
        //去 重
        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
        List<Message> list = resp.getPageData();
        if(list != null){
            for(Message m : list) {
                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
                for(HivePartitionPart[] ps : pss){
                    for(HivePartitionPart p : ps) {
                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {
                            set.add(p);
                        }

                    }
                }
            }
        }

        result.addAll(set);
        result.sort(new Comparator<HivePartitionPart>() {
            public int compare(HivePartitionPart a, HivePartitionPart b) {
                return -a.getValue().compareTo(b.getValue());
            }
        });

        return result;
    }


    // b_time
    private static void pullAndSortDescHivePartitionParts(
            MessageClientApi messageClientApi, String consumer,
            final String partitionFieldName,
            final Callback<List<HivePartitionPart>> callback,
            String... topic) {

        pullAndCommit(messageClientApi, consumer, new Callback<Resp<List<Message>>>() {
            public Boolean doCallback(Resp<List<Message>> resp) {
                return callback.doCallback(toHivePartitionPartsDesc(resp, partitionFieldName));
            }
        }, topic);
    }

    // b_time & partial commit
    private static void pullAndSortDescHivePartitionParts(
            MessageClientApi messageClientApi, String consumer,
            final String partitionFieldName,
            final HivePartitionPartsPartialCommitCallback<List<HivePartitionPart>> callback,
            String... topic) {

        pullAndCommit(messageClientApi, consumer, new PartialCommitCallback<Resp<List<Message>>>() {
            public List<Message> doCallback(Resp<List<Message>> resp) {

                List<HivePartitionPart> hivePartitions = new ArrayList<HivePartitionPart>();
                Map<HivePartitionPart, List<Message>> bTimeMappingMessages = new HashMap<>();
                //用Set去 重
                LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
                List<Message> list = resp.getPageData();
                if(list != null){
                    for(Message m : list) {
                        HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});

                        if(pss.length > 1) throw new RuntimeException("‘局部提交’不允许两个或两个以上的Hive分区（HivePartitionPart[]）在同一个消息中");

                        for(HivePartitionPart[] ps : pss){
                            for(HivePartitionPart p : ps) {
                                if(partitionFieldName.equals(p.getName()) && StringUtil.notEmpty(p.getValue()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue())) {

                                    List<Message> ms = bTimeMappingMessages.get(p);
                                    if(ms == null) {
                                        ms = new ArrayList<>();
                                        bTimeMappingMessages.put(p, ms);
                                    }
                                    ms.add(m);

                                    set.add(p);
                                }

                            }
                        }
                    }
                }

                hivePartitions.addAll(set);
                // 降序
                hivePartitions.sort(new Comparator<HivePartitionPart>() {
                    public int compare(HivePartitionPart a, HivePartitionPart b) {
                        return -a.getValue().compareTo(b.getValue());
                    }
                });

                HivePartitionPart[] needPartialCommitBTimes = callback.doCallback(hivePartitions);

                List<Message> needPartialCommitMessages = new ArrayList<>();
                for(HivePartitionPart b_time : needPartialCommitBTimes){
                    needPartialCommitMessages.addAll(bTimeMappingMessages.get(b_time));
                }
                return needPartialCommitMessages;
            }
        }, topic);
    }

    //拉取 & 升序
    public static void pullAndCommit(MessageClientApi messageClientApi, String consumer, Callback<Resp<List<Message>>> callback, String... topic){

        Resp<List<Message>> resp = messageClientApi.pullMessage(new MessagePullReq(consumer, topic));
        List<Message> pd = resp.getPageData();

        // 是否需要提交偏移
        boolean b = callback.doCallback(resp);

        if(b && pd != null && !pd.isEmpty()) {

            MessageConsumerCommitReq[] ms = new MessageConsumerCommitReq[pd.size()];
            // 其实只要提交每个topic的最大偏移的那一个，没必要提交多条req，待改
            for(int i=0; i < pd.size(); i++){
                ms[i] = new MessageConsumerCommitReq(consumer, pd.get(i).getTopic(), pd.get(i).getOffset());
            }
            messageClientApi.commitMessageConsumer(ms);
        }
    }

    //拉取 & 局部提交
    public static void pullAndCommit(MessageClientApi messageClientApi, String consumer, PartialCommitCallback<Resp<List<Message>>> callback, String... topic){

        Resp<List<Message>> resp = messageClientApi.pullMessage(new MessagePullReq(consumer, topic));
        List<Message> pd = resp.getPageData();

        List<Message> partialCommitMessages = callback.doCallback(resp);

        if(partialCommitMessages != null && !partialCommitMessages.isEmpty() && pd != null) {
            MessageConsumerCommitReq[] ms = new MessageConsumerCommitReq[partialCommitMessages.size()];

            //其实只要提交每个topic的最大偏移的那一个，没必要提交多条req，待改
            for(int i=0; i < partialCommitMessages.size(); i++){
                ms[i] = new MessageConsumerCommitReq(consumer, partialCommitMessages.get(i).getTopic(), partialCommitMessages.get(i).getOffset(), true);
            }
            messageClientApi.commitMessageConsumer(ms);
        }
    }

    //获取的是上上部分的l_time(s)
    public static void pullAndSortByLTimeDescTailHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("l_time", messageClientApi, consumer, callback, topics);
    }

    //获取的是上上部分的b_time(s)
    public static void pullAndSortByBTimeDescTailHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("b_time", messageClientApi, consumer, callback, topics);
    }
    //获取的是上上部分的b_date(s)
    public static void pullAndSortByBDateDescTailHivePartitionParts(MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("b_date", messageClientApi, consumer, callback, topics);
    }

    //获取的是在“上上”部分的l_time(s)
    public static void pullAndSortByPartitionFieldDescTailHivePartitionParts(String hivePartitionFieldName, MessageClientApi messageClientApi, String consumer, Callback<List<HivePartitionPart>> callback, String... topics){
        String partitionFieldName = hivePartitionFieldName;// "l_time";

        Resp<List<Message>> resp = messageClientApi.pullMessage(new MessagePullReq(consumer, topics));
        List<Message> pd = resp.getPageData();

//        boolean b = callback.doCallback(resp);
        List<HivePartitionPart> result = new ArrayList<HivePartitionPart>();

        //去重
        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
        List<Message> list = pd;//resp.getPageData();
        Map<HivePartitionPart, List<Message>> filters = new HashMap<HivePartitionPart, List<Message>>();
        if(list != null){
            for(Message m : list) {
                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
                for(HivePartitionPart[] ps : pss){
                    for(HivePartitionPart p : ps) {
                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {

                            set.add(p);

                            List<Message> ms = filters.get(p);
                            if(ms == null){
                                ms = new ArrayList<Message>();
                            }
                            ms.add(m);
                            filters.put(p, ms);
                        }

                    }
                }
            }
        }

        result.addAll(set);
        result.sort(new Comparator<HivePartitionPart>() {
            public int compare(HivePartitionPart a, HivePartitionPart b) {
                return -a.getValue().compareTo(b.getValue());
            }
        });

        if(result.size() <= 1) {
            return;
        }

        HivePartitionPart _p = result.get(0);
        //Exclude the first for Commit
        filters.remove(_p);
        //Remove first for callback
        result.remove(0);

        boolean b = callback.doCallback(result);

        if(b) {
            List<MessageConsumerCommitReq> ms = new ArrayList<MessageConsumerCommitReq>();

            Set<Map.Entry<HivePartitionPart, List<Message>>> es = filters.entrySet();

            //其实只要提交每个topic的最大偏移的那一个，没必要提交多条req，待改
            for(Map.Entry<HivePartitionPart, List<Message>> e : es) {
                for(Message m: e.getValue()) {
                    ms.add(new MessageConsumerCommitReq(consumer, m.getTopic(), m.getOffset()));
                }
            }

            messageClientApi.commitMessageConsumer(ms.toArray(new MessageConsumerCommitReq[0]));
        }
    }

    public static void pullUpdateable(MessageClientApi messageClientApi, String consumer, final Callback<Message> callback, String... topic){
        pullAndCommit(messageClientApi, consumer, new Callback<Resp<List<Message>>>() {
            public Boolean doCallback(Resp<List<Message>> resp) {
                List<Message> ms = resp.getPageData();
                if(ms != null && ms.size() > 0) {
                    callback.doCallback(ms.get(ms.size() -1));
                }
                return false;
            }
        }, topic);
    }

    public static void push(MessageClientApi messageClientApi, PushReq...pushReqs) {
        MessagePushReq[] reqs = new MessagePushReq[pushReqs.length];
        for(int i = 0; i < pushReqs.length; i++) {
            reqs[i] = new MessagePushReq(pushReqs[i].topic(), pushReqs[i].key());
        }
        messageClientApi.pushMessage(reqs);
    }

    public static interface CommitOffsetStrategy{
        Map<HivePartitionPart, List<Message>> callback(Map<HivePartitionPart, List<Message>> partitionAndMessageMap, List<HivePartitionPart> descHivePartitionParts);
    }

    public static interface CallbackRespStrategy{
        List<HivePartitionPart> callback(List<HivePartitionPart> callbackResp);
    }

    public static CommitOffsetStrategy TAIL_COMMIT_OFFSET_STRATEGY = new CommitOffsetStrategy(){
        public Map<HivePartitionPart, List<Message>> callback(Map<HivePartitionPart, List<Message>> partitionAndMessageMap, List<HivePartitionPart> descHivePartitionParts) {
            if(descHivePartitionParts.size() > 0) {
                partitionAndMessageMap.remove(descHivePartitionParts.get(0));
            }
            return partitionAndMessageMap;
        }
    };

    public static CallbackRespStrategy TAIL_CALLBACK_RESP_STRATEGY = new CallbackRespStrategy(){
        public List<HivePartitionPart> callback(List<HivePartitionPart> descHivePartitionParts) {
            if(descHivePartitionParts.size() > 0) {
                descHivePartitionParts.remove(0);
            }
            return descHivePartitionParts;
        }
    };

    //获取的是“上上”部分的hive分区(s)
    public static void pullAndSortByPartitionFieldDesc(
            String hivePartitionFieldName,
            MessageClientApi messageClientApi,
            String consumer,
            Callback<List<HivePartitionPart>> callback,
            CommitOffsetStrategy commitOffsetStrategy,
            CallbackRespStrategy callbackRespStrategy,
            String... topics){
//        pullAndSortDescHivePartitionParts(messageClientApi, consumer, "l_time", callback, topics);
        String partitionFieldName = hivePartitionFieldName;// "l_time";

        Resp<List<Message>> resp = messageClientApi.pullMessage(new MessagePullReq(consumer, topics));
        List<Message> pd = resp.getPageData();

//        boolean b = callback.doCallback(resp);
        List<HivePartitionPart> result = new ArrayList<HivePartitionPart>();

        //去重
        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
        List<Message> list = pd;//resp.getPageData();
        Map<HivePartitionPart, List<Message>> filters = new HashMap<HivePartitionPart, List<Message>>();
        if(list != null){
            for(Message m : list) {
                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
                for(HivePartitionPart[] ps : pss){
                    for(HivePartitionPart p : ps) {
                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {

                            set.add(p);

                            List<Message> ms = filters.get(p);
                            if(ms == null){
                                ms = new ArrayList<Message>();
                            }
                            ms.add(m);
                            filters.put(p, ms);
                        }

                    }
                }
            }
        }

        result.addAll(set);
        result.sort(new Comparator<HivePartitionPart>() {
            public int compare(HivePartitionPart a, HivePartitionPart b) {
                return -a.getValue().compareTo(b.getValue());
            }
        });

//        if(result.size() <= 1) {
//            return;
//        }

//        HivePartitionPart _p = result.get(0);
//        //Excu'j'r'jlude the first for Commit
//        filters.remove(_p);
        filters = commitOffsetStrategy.callback(filters, result);

        //Remove first for callback
//        result.remove(0);
        result = callbackRespStrategy.callback(result);

        boolean b = callback.doCallback(result);

        if(b) {
            List<MessageConsumerCommitReq> ms = new ArrayList<MessageConsumerCommitReq>();

            Set<Map.Entry<HivePartitionPart, List<Message>>> es = filters.entrySet();

            //其实只要提交每个topic的最大偏移的那一个，没必要提交多条req，待改
            for(Map.Entry<HivePartitionPart, List<Message>> e : es) {
                for(Message m: e.getValue()) {
                    ms.add(new MessageConsumerCommitReq(consumer, m.getTopic(), m.getOffset()));
                }
            }

            if(ms.size() > 0) {
                messageClientApi.commitMessageConsumer(ms.toArray(new MessageConsumerCommitReq[0]));
            }
        }
    }

    public interface Callback<T> {
        Boolean doCallback(T resp);
    }

    public interface HivePartitionPartsPartialCommitCallback<T> {
        /**
         * @return 需要‘局部提交’偏移的HivePartition，最后回局部提交该HivePartition对应Messages偏移
         */
        HivePartitionPart[] doCallback(T resp);
    }

    public interface PartialCommitCallback<T> {
        /**
         * @return 需要‘局部提交’偏移的Messages
         */
        List<Message> doCallback(T resp);
    }


    //Test
    public static void main(String[] args) {

//        MessageClient mc = new MessageClient("http://node14:5555");
//        MessageClientUtil.pullAndSortByLTimeDescHivePartitionParts(mc, "test_cer", new MessageClientUtil.Callback<ArrayList<HivePartitionPart>>(){
//
//            public Boolean doCallback(ArrayList<HivePartitionPart> resp) {
//                System.out.println(OM.toJOSN(resp));
//                return false;
//            }
//        }, "test_topic" );

        MessageClientApi mc = new MessageClientApi("http://node14:5555");
        JavaMessageClient.pullAndSortByLTimeDescTailHivePartitionParts(mc, "test_cer", new JavaMessageClient.Callback<List<HivePartitionPart>>(){

            public Boolean doCallback(List<HivePartitionPart> resp) {
                System.out.println(OM.toJOSN(resp));
                return true;
            }
        }, "test_topic" );

    }
}

