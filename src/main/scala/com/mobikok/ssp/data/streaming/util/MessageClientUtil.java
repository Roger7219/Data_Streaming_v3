package com.mobikok.ssp.data.streaming.util;


import com.fasterxml.jackson.core.type.TypeReference;
import com.mobikok.message.*;
import com.mobikok.message.client.MessageClient;
import com.mobikok.ssp.data.streaming.entity.HivePartitionPart;

import java.util.*;

public class MessageClientUtil {

    public static void pullAndSortByLTimeDescHivePartitionParts(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClient, consumer, "l_time", callback, topics);
    }

    public static void pullAndSortByBDateDescHivePartitionParts(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
        pullAndSortDescHivePartitionParts(messageClient, consumer, "b_date",callback, topics);
    }

    private static void pullAndSortDescHivePartitionParts(
            MessageClient messageClient, String consumer,
            final String partitionFieldName,
            final Callback<ArrayList<HivePartitionPart>> callback,
            String... topic){

        pullAndCommit(messageClient, consumer, new Callback<Resp<List<Message>>>() {

            public Boolean doCallback(Resp<List<Message>> resp) {

                ArrayList<HivePartitionPart> result = new ArrayList<HivePartitionPart>();

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

                return callback.doCallback(result);

            }
        }, topic);
    }

    //按offset升序
    public static void pullAndCommit(MessageClient messageClient, String consumer, Callback<Resp<List<Message>>> callback, String... topic){

        Resp<List<Message>> resp = messageClient.pullMessage(new MessagePullReq(consumer, topic));
        List<Message> pd = resp.getPageData();

        boolean b = callback.doCallback(resp);

        if(b && pd != null) {
            MessageConsumerCommitReq[] ms = new MessageConsumerCommitReq[pd.size()];

            for(int i=0; i < pd.size(); i++){
                ms[i] = new MessageConsumerCommitReq(consumer, pd.get(i).getTopic(), pd.get(i).getOffset());
            }
            messageClient.commitMessageConsumer(ms);
        }
    }

    //获取的是上上部分的l_time(s)
    public static void pullAndSortByLTimeDescTailHivePartitionParts(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("l_time", messageClient, consumer, callback, topics);
    }

    //获取的是上上部分的b_time(s)
    public static void pullAndSortByBTimeDescTailHivePartitionParts(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("b_time", messageClient, consumer, callback, topics);
    }
    //获取的是上上部分的b_date(s)
    public static void pullAndSortByBDateDescTailHivePartitionParts(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
        pullAndSortByPartitionFieldDescTailHivePartitionParts("b_date", messageClient, consumer, callback, topics);
    }

    //获取的是上上部分的l_time(s)
    public static void pullAndSortByPartitionFieldDescTailHivePartitionParts(String hivePartitionFieldName, MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
//        pullAndSortDescHivePartitionParts(messageClient, consumer, "l_time", callback, topics);
        String partitionFieldName = hivePartitionFieldName;// "l_time";

        Resp<List<Message>> resp = messageClient.pullMessage(new MessagePullReq(consumer, topics));
        List<Message> pd = resp.getPageData();

//        boolean b = callback.doCallback(resp);
        ArrayList<HivePartitionPart> result = new ArrayList<HivePartitionPart>();

        //去重
        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
        List<Message> list = pd;//resp.getPageData();
        Map<HivePartitionPart, ArrayList<Message>> filters = new HashMap<HivePartitionPart, ArrayList<Message>>();
        if(list != null){
            for(Message m : list) {
                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
                for(HivePartitionPart[] ps : pss){
                    for(HivePartitionPart p : ps) {
                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {

                            set.add(p);

                            ArrayList<Message> ms = filters.get(p);
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

            Set<Map.Entry<HivePartitionPart, ArrayList<Message>>> es = filters.entrySet();

            for(Map.Entry<HivePartitionPart, ArrayList<Message>> e : es) {
                for(Message m: e.getValue()) {
                    ms.add(new MessageConsumerCommitReq(consumer, m.getTopic(), m.getOffset()));
                }
            }

            messageClient.commitMessageConsumer(ms.toArray(new MessageConsumerCommitReq[0]));
        }
    }

    public static void pullUpdateable(MessageClient messageClient, String consumer, final Callback<Message> callback, String... topic){
        pullAndCommit(messageClient, consumer, new Callback<Resp<List<Message>>>() {
            public Boolean doCallback(Resp<List<Message>> resp) {
                List<Message> ms = resp.getPageData();
                if(ms != null && ms.size() > 0) {
                    callback.doCallback(ms.get(ms.size() -1));
                }
                return false;
            }
        }, topic);
    }

    public static void push(MessageClient messageClient, PushReq...pushReqs) {
        MessagePushReq[] reqs = new MessagePushReq[pushReqs.length];
        for(int i = 0; i < pushReqs.length; i++) {
            reqs[i] = new MessagePushReq(pushReqs[i].topic(), pushReqs[i].key());
        }
        messageClient.pushMessage(reqs);
    }

    public static interface CommitOffsetStrategy{
        Map<HivePartitionPart, ArrayList<Message>> callback(Map<HivePartitionPart, ArrayList<Message>> partitionAndMessageMap, ArrayList<HivePartitionPart> descHivePartitionParts);
    }

    public static interface CallbackRespStrategy{
        ArrayList<HivePartitionPart> callback(ArrayList<HivePartitionPart> callbackResp);
    }

    public static CommitOffsetStrategy TAIL_COMMIT_OFFSET_STRATEGY = new CommitOffsetStrategy(){
        public Map<HivePartitionPart, ArrayList<Message>> callback(Map<HivePartitionPart, ArrayList<Message>> partitionAndMessageMap, ArrayList<HivePartitionPart> descHivePartitionParts) {
            if(descHivePartitionParts.size() > 0) {
                partitionAndMessageMap.remove(descHivePartitionParts.get(0));
            }
            return partitionAndMessageMap;
        }
    };

    public static CallbackRespStrategy TAIL_CALLBACK_RESP_STRATEGY = new CallbackRespStrategy(){
        public ArrayList<HivePartitionPart> callback(ArrayList<HivePartitionPart> descHivePartitionParts) {
            if(descHivePartitionParts.size() > 0) {
                descHivePartitionParts.remove(0);
            }
            return descHivePartitionParts;
        }
    };

    //获取的是上上部分的l_time(s)
    public static void pullAndSortByPartitionFieldDesc(
            String hivePartitionFieldName,
            MessageClient messageClient,
            String consumer,
            Callback<ArrayList<HivePartitionPart>> callback,
            CommitOffsetStrategy commitOffsetStrategy,
            CallbackRespStrategy callbackRespStrategy,
            String... topics){
//        pullAndSortDescHivePartitionParts(messageClient, consumer, "l_time", callback, topics);
        String partitionFieldName = hivePartitionFieldName;// "l_time";

        Resp<List<Message>> resp = messageClient.pullMessage(new MessagePullReq(consumer, topics));
        List<Message> pd = resp.getPageData();

//        boolean b = callback.doCallback(resp);
        ArrayList<HivePartitionPart> result = new ArrayList<HivePartitionPart>();

        //去重
        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
        List<Message> list = pd;//resp.getPageData();
        Map<HivePartitionPart, ArrayList<Message>> filters = new HashMap<HivePartitionPart, ArrayList<Message>>();
        if(list != null){
            for(Message m : list) {
                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
                for(HivePartitionPart[] ps : pss){
                    for(HivePartitionPart p : ps) {
                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {

                            set.add(p);

                            ArrayList<Message> ms = filters.get(p);
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

            Set<Map.Entry<HivePartitionPart, ArrayList<Message>>> es = filters.entrySet();

            for(Map.Entry<HivePartitionPart, ArrayList<Message>> e : es) {
                for(Message m: e.getValue()) {
                    ms.add(new MessageConsumerCommitReq(consumer, m.getTopic(), m.getOffset()));
                }
            }

            if(ms.size() > 0) {
                messageClient.commitMessageConsumer(ms.toArray(new MessageConsumerCommitReq[0]));
            }
        }
    }

//    //获取的是上上部分的l_time(s)
//    public static void pullAndSortByLTimeDescTailHivePartitionPartsReturnAll(MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
//        pullAndSortByPartitionFieldDescTailHivePartitionPartsReturnAll("l_time", messageClient, consumer, callback, topics);
//    }

//    //获取的是上上部分的l_time(s)
//    public static void pullAndSortByPartitionFieldDescTailHivePartitionPartsReturnAll(String hivePartitionFieldName, MessageClient messageClient, String consumer, Callback<ArrayList<HivePartitionPart>> callback, String... topics){
////        pullAndSortDescHivePartitionParts(messageClient, consumer, "l_time", callback, topics);
//        String partitionFieldName = hivePartitionFieldName;// "l_time";
//
//        Resp<List<Message>> resp = messageClient.pullMessage(new MessagePullReq(consumer, topics));
//        List<Message> pd = resp.getPageData();
//
////        boolean b = callback.doCallback(resp);
//        ArrayList<HivePartitionPart> result = new ArrayList<HivePartitionPart>();
//
//        //去重
//        LinkedHashSet<HivePartitionPart> set = new LinkedHashSet<HivePartitionPart>();
//        List<Message> list = pd;//resp.getPageData();
//        Map<HivePartitionPart, ArrayList<Message>> filters = new HashMap<HivePartitionPart, ArrayList<Message>>();
//        if(list != null){
//            for(Message m : list) {
//                HivePartitionPart[][] pss = OM.toBean(m.getKeyBody(), new TypeReference <HivePartitionPart[][]>() {});
//                for(HivePartitionPart[] ps : pss){
//                    for(HivePartitionPart p : ps) {
//                        if(partitionFieldName.equals(p.getName()) && !"__HIVE_DEFAULT_PARTITION__".equals(p.getValue()) && StringUtil.notEmpty(p.getValue())) {
//
//                            set.add(p);
//
//                            ArrayList<Message> ms = filters.get(p);
//                            if(ms == null){
//                                ms = new ArrayList<Message>();
//                            }
//                            ms.add(m);
//                            filters.put(p, ms);
//                        }
//
//                    }
//                }
//            }
//        }
//
//        result.addAll(set);
//        result.sort(new Comparator<HivePartitionPart>() {
//            public int compare(HivePartitionPart a, HivePartitionPart b) {
//                return -a.getValue().compareTo(b.getValue());
//            }
//        });
//
//        if(result.size() <= 1) {
//            return;
//        }
//
//        HivePartitionPart _p = result.get(0);
//        //Exclude the first for Commit
//        filters.remove(_p);
//        //Remove first for callback
////        result.remove(0);
//
//        boolean b = callback.doCallback(result);
//
//        if(b) {
//            List<MessageConsumerCommitReq> ms = new ArrayList<MessageConsumerCommitReq>();
//
//            Set<Map.Entry<HivePartitionPart, ArrayList<Message>>> es = filters.entrySet();
//
//            for(Map.Entry<HivePartitionPart, ArrayList<Message>> e : es) {
//                for(Message m: e.getValue()) {
//                    ms.add(new MessageConsumerCommitReq(consumer, m.getTopic(), m.getOffset()));
//                }
//            }
//
//            messageClient.commitMessageConsumer(ms.toArray(new MessageConsumerCommitReq[0]));
//        }
//    }


    public interface Callback<T> {
        Boolean doCallback(T resp);
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

        MessageClient mc = new MessageClient("http://node14:5555");
        MessageClientUtil.pullAndSortByLTimeDescTailHivePartitionParts(mc, "test_cer", new MessageClientUtil.Callback<ArrayList<HivePartitionPart>>(){

            public Boolean doCallback(ArrayList<HivePartitionPart> resp) {
                System.out.println(OM.toJOSN(resp));
                return true;
            }
        }, "test_topic" );

    }
}

