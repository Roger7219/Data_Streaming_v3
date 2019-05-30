package com.mobikok.ssp.data.streaming.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class KafkaSender {

    //nadx-kafka
    public static String KAFKA_BOOTSTRAP_SERVERS = "node187:6667,node195:6667,node196:6667";
//	public static final String KAFKA_BOOTSTRAP_SERVERS = "104.250.147.226:6667,104.250.147.106:6667,104.250.150.18:6667";

    private static String kafkaStoredMessageBaseDir = "/data/kafka_stored_message";
//	public static final String KAFKA_BOOTSTRAP_SERVERS = "192.168.111.14:6667,192.168.111.15:6667,192.168.111.17:6667";
//	public static final String KAFKA_BOOTSTRAP_SERVERS = "node14:6667,node15:6667,node17:6667";
//	public static final String KAFKA_BOOTSTRAP_SERVERS = "node30:6667,node31:6667,node32:6667";

//	public static final String KAFKA_BOOTSTRAP_SERVERS = "node14:9222";

    public static final boolean KAFKA_PRODUCER_IS_ASYNC = true;
    public static final String KAFKA_ACKS = "-1";
    public static final String KAFKA_KEY_SERIALIZER = StringSerializer.class.getName();
    public static final String KAFKA_VALUE_SERIALIZER = StringSerializer.class.getName();
    public static final Integer KAFKA_RETRIES = 0;
    public static final Integer KAFKA_MAX_BLOCK_MS = 0;
    public static final Integer KAFKA_CONNECTIONS_MAX_IDLE_MS = 1000*60;
    public static final Integer KAFKA_BUFFER_MEMORY_B = 1024*1024*64;//64M
    public static final Integer KAFKA_LINGER_MS = 1000*2;//2s
    public static final Integer KAFKA_BATCH_SIZE_B = 1024*16*10*2;//160k

    private static final int LOG_INTERVAL_TIME_MS = 1000*20;// 打印日志时间间隔
    public static final Integer RESEND_INTERVAL_MS = 1000*60*5; //1000*60*5;//5分钟
    static final int WRITING_SIGN_EXPIRE_TIME_MS = RESEND_INTERVAL_MS; ///= 1000*60*5;   //5分钟
    static final int MAX_DELETE_SIGN_EXPIRE_TIME_MS = 1000*60*60*24*2;//2天
    static final int MAX_DELETE_SIGN_KEEP_FILES = 10;//待删除文件 最多保留个数
    static final int MAX_FILE_STORE_SIZE_MB = 100;//100MB

    public static void main(String[] args) throws InterruptedException, IOException {
        String fn = "topic.traffic_v9__T__20190429_022433195__P__18976.writing";
        String FILE_NAME_TOPIC_SEPARATOR = "&";
        String PID_PREFIX = "^";
        String signStr = fn.substring(fn.indexOf(FILE_NAME_TOPIC_SEPARATOR) + 1, fn.indexOf(PID_PREFIX));

//        String fn = "115.8128.delete";
//
//        String _fn = new StringBuffer(fn).reverse().toString();
//        String pid = null;
//        //文件名中提取pid
//        int i = _fn.indexOf(new StringBuffer(".delete").reverse().toString()) + ".delete".length();
//        int i2  = _fn.indexOf(".",  i+1);
//        if(i >= 0 && i2 >= 0) {
//            pid = new StringBuffer(_fn.substring(i, i2)).reverse().toString();
//        }
//        System.out.println(pid);
//
//
//        KafkaSender k = new KafkaSender("test1", "test1"/*, Level.DEBUG*/);
//        k.send();
//		//
//		int i=0;
//		while (true) {
//
//			k.send("topic_KafkaUtilTest_01", "axxxxxxxxxxxxxxxxxxxxxxxxxxx_" +  i);
//			i++;
//			Thread.sleep(1000*10*1);
//		}

//		Thread.sleep(100000000000L);
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSender.class);

    private boolean isClosed = false;

    private String topic;
    private Properties customProducerConfig = null;
    private volatile KafkaProducer<String, String> producer;

    private volatile KafkaStoredMessageManager kafkaStoredMessageManager = null;

    volatile private Map<Long, Long> sendRequests = new ConcurrentHashMap<Long, Long>();
    volatile private Map<Long, Long> sendFailures = new ConcurrentHashMap<Long, Long>();

    volatile private Map<Long, Long> reSendRequests = new ConcurrentHashMap<Long, Long>();
    volatile private Map<Long, Long> sendSuccesses = new ConcurrentHashMap<Long, Long>();

    volatile private static Map<String, KafkaSender> SINGLETON_INSTANCE_MAP = new ConcurrentHashMap<String, KafkaSender>();

    public static synchronized KafkaSender instance(String topic){
        return instance(topic, null);
    }
    public static synchronized KafkaSender instance(String topic, Properties producerConfig){
        KafkaSender ks = SINGLETON_INSTANCE_MAP.get(topic);
        if(ks == null) {
            ks = new KafkaSender(topic, topic, producerConfig);
            SINGLETON_INSTANCE_MAP.put(topic, ks);
        }
        return ks;

    }

    private KafkaSender(String topic){
        this(topic, topic/*, Level.INFO*/);
    }

    public KafkaSender(String topic, String sendFailMessagesStoreSubDir/*, Level logLevel*/){
//		LOG.setLevel(logLevel);
//		this.topic = topic;
//		kafkaStoredMessageManager = new KafkaStoredMessageManager(kafkaStoredMessageBaseDir + java.io.File.separator + sendFailMessagesStoreSubDir, logLevel);
//
//		initHeartbeat();
        this(topic, sendFailMessagesStoreSubDir, /*logLevel,*/ null);
    }

    private static String logMsg(String topic, Object... msg) {
        StringBuffer buff = new StringBuffer();
        buff.append('[').append(topic).append("] ");
        for(Object m : msg) {
            if(m instanceof Throwable) {
                Throwable t = ((Throwable)m);
                buff.append(t.getClass().getName()).append(": ").append(t.getMessage());
            }else {
                buff.append(String.valueOf(m));
            }
        }
        return buff.toString();
    }
    private void logError(Throwable throwable, Object... msg){LOG.error(logMsg(topic, msg), throwable);}
    private void logInfo(Object... msg){LOG.info(logMsg(topic, msg));}
    private void logWarn(Object... msg){LOG.warn(logMsg(topic, msg));}
    private void logDebug(Object... msg){LOG.debug(logMsg(topic, msg));}

    public KafkaSender(final String topic, String sendFailMessagesStoreSubDir/*, Level logLevel*/, Properties customProducerConfig){

//        LOG.setLevel(logLevel);
//        Logger.getLogger("org.apache.kafka").setLevel(logLevel);
        this.topic = topic;
        this.kafkaStoredMessageManager = new KafkaStoredMessageManager(topic, kafkaStoredMessageBaseDir + File.separator + sendFailMessagesStoreSubDir/*, logLevel*/);
        this.customProducerConfig = customProducerConfig;

        initHeartbeat();

//        try{
//            changStatusAndReSend(false);
//        }catch (Throwable t) {
////			LOG.error("["+topic+"] Retrieve to send error",t);
//            logError(t, "Retrieve to send error");
//        }

        logWarn("KafkaSender init done !!!!!!");
//		LOG.warn("["+topic+"] Init-ReSend done !!");

        //
        new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {

                        changStatusAndReSend(true);

                    }catch(Throwable t) {
//						t.printStackTrace();
//						LOG.error("["+topic+"] Retrieve to send error",t);
                        logError(t, "Retrieve to send error");
                    }finally {
                        //间隔时间
                        try {
                            Thread.sleep(RESEND_INTERVAL_MS);
                        } catch (InterruptedException e) { }
                    }
                }
            }
        }).start();

    }

    private void initHeartbeat(){
        createIfInvalidProducer();

        //定时flush
        new Thread(new Runnable() {
            public void run() {

                while (true) {
                    try {
                        if(producer != null) {
                            producer.flush();
                        }
                        Thread.sleep(1000*3);
                    }catch (Throwable t) {
//						LOG.warn("Producer flush fail", t);
                        logError(t,"Producer flush fail");
                    }
                }
            }
        }).start();

        //心跳
        new Thread(new Runnable() {
            public void run() {

                while (true) {
                    try{
                        if(System.currentTimeMillis() - lastLogTime2 > LOG_INTERVAL_TIME_MS) {
                            LOG.info(sendLag());
                            lastLogTime2 = System.currentTimeMillis();
                        }
                        Thread.sleep(1000*1);
                    }catch (Throwable t) {
//						LOG.warn("SendLag heartbeat error", t);
                        logError(t,"SendLag heartbeat error");
                    }
                }
            }
        }).start();
    }

    private void changStatusAndReSend(final Boolean needIncrReSendRequests) throws IOException{
        kafkaStoredMessageManager.changeStatusToReadingIfWritingCompleted();
        //获取并移出项
        kafkaStoredMessageManager.retrieve(new KafkaStoredMessageManagerRetrieveCallback(){
            public void doCallback(String topic, List<KafkaStoredMessage> kafkaStoredMessages) {
//				LOG.warn("Retrieve re-send to kafka starting, size: " + kafkaStoredMessages.size());

                List<String> msgs = new ArrayList<String>(kafkaStoredMessages.size());
                for(int i = 0; i < kafkaStoredMessages.size(); i++) {
                    msgs.add(kafkaStoredMessages.get(i).getMsg());
                }

                if(needIncrReSendRequests) {
                    incrReSendRequests(msgs.size());
                }
                reSend( msgs);

//				LOG.warn("Retrieve re-send to kafka done, size: " + kafkaStoredMessages.size());
                logWarn("Retrieve re-send to kafka done, size: ", kafkaStoredMessages.size());
            }
        });

    }

    private void incrReSendRequests(long size) {
        Long s = reSendRequests.get(Thread.currentThread().getId());
        if (s == null) {
            s = 0L;
        }
        reSendRequests.put(Thread.currentThread().getId(), s + size);
    }

    private void incrSendRequests(long size) {
        Long s =sendRequests.get(Thread.currentThread().getId());
        if (s == null) {
            s = 0L;
        }
        sendRequests.put(Thread.currentThread().getId(), s + size);
    }

    private void incrSendSuccesses(long size) {
        Long s = sendSuccesses.get(Thread.currentThread().getId());
        if(s == null) {
            s = 0L;
        }
        sendSuccesses.put(Thread.currentThread().getId(), s + size);
    }

    private void incrSendFailures(long size) {
        Long s = sendFailures.get(Thread.currentThread().getId());
        if(s == null) {
            s = 0L;
        }
        sendFailures.put(Thread.currentThread().getId(), s + size);
    }

    private String sendLag(){
        long r = 0;
        long s = 0;
        long reSends = 0;
        long sendFs = 0;

        for(Map.Entry<Long,Long> sr : sendRequests.entrySet()) {
            r += sr.getValue();
        }
        for(Map.Entry<Long,Long> ss : sendSuccesses.entrySet()) {
            s += ss.getValue();
        }
        for(Map.Entry<Long,Long> rss : reSendRequests.entrySet()) {
            reSends += rss.getValue();
        }
        for(Map.Entry<Long,Long> fs : sendFailures.entrySet()) {
            sendFs += fs.getValue();
        }

        return new StringBuilder()
                .append("Send lag of topic ")
                .append(topic)
                .append(": ")
                .append(r - s - sendFs)  // 正在发送的数量
                .append(" (")
                .append(r)
                .append(" - ")
                .append(s)
                .append(" - ")
                .append(sendFs)
                .append("), will-re-send: ")
                .append(sendFs - reSends)
                .append(", re-sended: ")
                .append(reSends)
                .append("\nall-send-threads        : ")
                .append(sendRequests.size())
                .append("\nsuccess callback threads: ")
                .append(sendSuccesses.size())
                .append("\nre-send threads         : ")
                .append(reSendRequests.size())
                .append("\nsending late            : ")
                .append( currBatchSendTime == -1 ? 0 : (System.currentTimeMillis()- currBatchSendTime)/1000.0 )
                .append( "s, count: " + currSendSize)

                .append("\nre-sending late         : ")
                .append( currBatchReSendStartTime == -1 ? 0 : (System.currentTimeMillis()- currBatchReSendStartTime)/1000.0 )
                .append( "s, count: " + currReSendSize)

                .append(currSendThread == null ? "" : "\nsend thread             : ")
                .append(currSendThread == null ? "" : Arrays.deepToString(currSendThread.getStackTrace()))

                .append(currReSendThread == null ? "" : "\nre-send thread          : ")
                .append(currReSendThread == null ? "" : Arrays.deepToString(currReSendThread.getStackTrace()))

                .toString();

//		return "Send lag: " + (r - s - sendFs) + " ("+r+" - "+s+" - "+sendFs+"), re-send: "+reSends+", all-send threads: " + sendRequests.size() + ", callback success threads: " + sendSuccesses.size() + ", re-send threads:" + reSendRequests.size();
    }

    private void createIfInvalidProducer() {
        if(producer == null || isClosed) {
//			LOG.info("Creating new kafka producer");
            logInfo("Creating new kafka producer");
            producer = new KafkaProducer<String, String>(getProducerConfig());
//			LOG.info("Created new kafka producer");
            logInfo("Created new kafka producer");
        }
    }

    public void send(String message) {
        send(Collections.singletonList(message));
    }

    volatile private long currBatchSendTime = -1;
    volatile private Thread currSendThread;
    volatile private long currSendSize = 0;

    public void send(List<String> messages) {
        try {
            currSendThread = Thread.currentThread();
            createIfInvalidProducer();

            int size = messages.size();
            currSendSize = size;
            incrSendRequests(size);

            if(System.currentTimeMillis() - lastLogTime4 > LOG_INTERVAL_TIME_MS) {
//				LOG.info("Sending to kafka " + size + " message(s)");
                logInfo("Sending to kafka ", size, " message(s)");
                lastLogTime4 = System.currentTimeMillis();
            }
            currBatchSendTime = System.currentTimeMillis();

            if (KAFKA_PRODUCER_IS_ASYNC) {
                for(String m : messages){
                    producer.send(new ProducerRecord<String, String>(topic, null, m), new MessageProduceCallback(size, System.currentTimeMillis(), topic, null, m));
                }
            }else {
                for(String m : messages){
                    producer.send(new ProducerRecord<String, String>(topic, null, m)).get();
                }
            }
            if(System.currentTimeMillis() - lastLogTime5 > LOG_INTERVAL_TIME_MS) {
                logInfo("Send batch use time: ", (System.currentTimeMillis() - currBatchSendTime)/1000F, "s, count: ", messages.size());
//				LOG.info("Send batch use time: " + (System.currentTimeMillis() - currBatchSendTime)/1000F+"s, count: " + messages.length );
                lastLogTime5 = System.currentTimeMillis();
            }
            currBatchSendTime = -1;
            currSendSize = 0;

            //producer.flush();
        }catch(Throwable e) {
            closeProducer(e);
        }
    }

    volatile private long currBatchReSendStartTime = -1;
    volatile private Thread currReSendThread;
    volatile private long currReSendSize = 0;

    private void reSend(List<String> messages) {
        try {
            currReSendThread = Thread.currentThread();
            createIfInvalidProducer();

            int size = messages.size();
            currReSendSize = size;
            incrSendRequests(size);

            if(System.currentTimeMillis() - lastLogTime4 > LOG_INTERVAL_TIME_MS) {
//				LOG.info("Sending to kafka " + size + " message(s)");
                logInfo("Sending to kafka ", size, " message(s)");
                lastLogTime4 = System.currentTimeMillis();
            }

            currBatchReSendStartTime = System.currentTimeMillis();

            if (KAFKA_PRODUCER_IS_ASYNC) {
                for(String m : messages){
                    producer.send(new ProducerRecord<String, String>(topic, null, m), new MessageProduceCallback(size, System.currentTimeMillis(), topic, null, m));
                }
            }else {
                for(String m : messages){
                    producer.send(new ProducerRecord<String, String>(topic, null, m)).get();
                }
            }

            if(System.currentTimeMillis() - lastLogTime5 > LOG_INTERVAL_TIME_MS) {
//				LOG.info("Send batch use time: " + (System.currentTimeMillis() - currBatchReSendStartTime)/1000F+"s, count: " + messages.length );
                logInfo("Send batch use time: ", (System.currentTimeMillis() - currBatchReSendStartTime)/1000F, "s, count: ", messages.size());
                lastLogTime5 = System.currentTimeMillis();
            }

            currBatchReSendStartTime = -1;
            currReSendSize = 0;
            //producer.flush();
        }catch(Throwable e) {
            closeProducer(e);
        }
    }

    private void closeProducer(){
        closeProducer(null);
    }

    private void closeProducer(Throwable e){
        if(e != null) {
            logError(e, "Exception when sending Kafka messages, Will flush-close producer !!!");
//			LOG.error("Exception when sending Kafka messages, Will flush-close producer !!!", e);
        }else {
            logWarn("Will flush-close producer !");
//			LOG.warn("Will flush-close producer !");
        }

        KafkaProducer<String, String> p = producer;
        try{
            isClosed = true;
            producer = null;
            p.flush();
        }catch (Throwable t){
            logError(e, "An exception occurs when flush producer");
//			LOG.error("An exception occurs when flush producer", e);
        }finally {
            try {
                if(p != null) {
                    p.close();
                }
            }catch (Throwable t) {
                logError(e,"An exception occurs when close producer");
//				LOG.error("An exception occurs when close producer", e);
            }
        }
    }

    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ProducerConfig.ACKS_CONFIG, KAFKA_ACKS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_VALUE_SERIALIZER);

        //pro
        props.put(ProducerConfig.RETRIES_CONFIG, KAFKA_RETRIES);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, KAFKA_MAX_BLOCK_MS);
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, KAFKA_CONNECTIONS_MAX_IDLE_MS);

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, KAFKA_BUFFER_MEMORY_B);
        props.put(ProducerConfig.LINGER_MS_CONFIG, KAFKA_LINGER_MS);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, KAFKA_BATCH_SIZE_B);

        // compressType有四种取值:none lz4 gzip snappy
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,  "lz4");

        //dev
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 100);
//	props.put(ProducerConfig.RETRIES_CONFIG, 0/*Integer.MAX_VALUE*/);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000 /*Long.MAX_VALUE*/);
//	props.put("block.on.buffer.full", true);
//	props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 50);
//	props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000*10/*1000*60*30*/);

        //覆盖默认
        if(customProducerConfig != null) {
            props.putAll(customProducerConfig);
        }
        return props;
    }

    // ==============================================================================================

    public static interface KafkaStoredMessageManagerRetrieveCallback {
        void doCallback(String topic, List<KafkaStoredMessage> kafkaStoredMessages);

    }
    volatile private long lastLogTime = 0L;
    volatile private long lastLogTime2 = 0L;
    volatile private long lastLogTime3 = 0L;
    volatile private long lastLogTime4 = 0L;
    volatile private long lastLogTime5 = 0L;

    public class MessageProduceCallback implements org.apache.kafka.clients.producer.Callback {
        private final long startTime;
        private final String key;
        private KafkaStoredMessage storedKafkaMessage;
        private String topic;

        public MessageProduceCallback(int batchSzie, long startTime, String topic, String key, String msg) {
            this.startTime = startTime;
            this.key = key;
            this.topic = topic;
            storedKafkaMessage = new KafkaStoredMessage(topic, msg);
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            if(e != null) {
                incrSendFailures(1);
                if(System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_TIME_MS) {
                    logInfo("Send fail, error: ", e.getClass().getName(), ": ", e.getMessage());
//					LOG.info( "["+topic+"] Send fail, error: " +e.getClass().getName()+ ": " + e.getMessage());
                    lastLogTime = System.currentTimeMillis();
                }

                if(LOG.isDebugEnabled()) {
                    logError(e, "Fail message: \n", JSON.toJOSN(storedKafkaMessage), "\nerror: ");
//					LOG.debug("["+topic+"] Fail message: \n"+ JSON.toJOSN(storedKafkaMessage) +"\nerror: ", e);
                }
                //加入元素
                try {
                    kafkaStoredMessageManager.store(topic, storedKafkaMessage);
                } catch (Exception ex) {
                    logError(e,"Store fail:");
//					LOG.error("["+topic+"] Store fail:", ex);
                }

            }else {
                if(System.currentTimeMillis() - lastLogTime3 > LOG_INTERVAL_TIME_MS) {
                    logInfo("Send success !");
//					LOG.info( "["+topic+"] Send success !");
                    lastLogTime3 = System.currentTimeMillis();
                }

                incrSendSuccesses(1);
                if (recordMetadata != null && LOG.isDebugEnabled()) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
//					LOG.debug("["+topic+"] Kafka send succeeded, message : " + storedKafkaMessage.getMsg()
//							+ ", partition id: "
//							+ recordMetadata.partition() + ", elapsedTime: "
//							+ elapsedTime);

                    logDebug("Kafka send succeeded, message : ", storedKafkaMessage.getMsg(),
                            ", partition id: ", recordMetadata.partition(),
                            ", elapsedTime: ", elapsedTime
                    );
                }
            }

        }

    }

    public static class KafkaStoredMessage {
        //		private String topic;
        private String msg;

        public KafkaStoredMessage(){}

        public KafkaStoredMessage(String topic, String msg) {
//			this.topic = topic;
            this.msg = msg;
        }
        //		public String getTopic() {
//			return topic;
//		}
        public String getMsg() {
            return msg;
        }
        //		public void setTopic(String topic) {
//			this.topic = topic;
//		}
        public void setMsg(String msg) {
            this.msg = msg;
        }
    }

    public static class KafkaStoredMessageManager {

        private static final Logger LOG = LoggerFactory.getLogger(KafkaStoredMessageManager.class);
        private static final String RECORD_SEPARATOR = "\r\n\r\n";
        private static final Object WRITE_LOCK = new Object();
        private static final Object READ_LOCK = new Object();

        private static final String FILE_NAME_TOPIC_SEPARATOR = "__T__";

        private static final String PID_PREFIX = "__P__";

        private ThreadLocal<DateFormat> DF = new ThreadLocal<DateFormat>() {
            protected DateFormat initialValue(){
                return new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
            }
        };
//      private static final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");

        volatile private Long lastWritingSignCreateTime = null;
        volatile private OutputStream currentOutputStream = null;
        volatile private String currOutputStreamPath = null;
        private String kafkaStoredMessageDir;
        volatile boolean isCurrentOutputStreamWriting = false;
        volatile String topic;

        volatile private ProcessMonitor processMonitor;

        private void logError(Throwable throwable, Object... msg){LOG.error(logMsg(topic, msg), throwable);}
        private void logError(Object... msg){LOG.error(logMsg(topic, msg));}
        private void logInfo(Object... msg){LOG.warn(logMsg(topic, msg));}
        private void logWarn(Object... msg){LOG.warn(logMsg(topic, msg));}
        private void logDebug(Object... msg){LOG.info(logMsg(topic, msg));}

        public KafkaStoredMessageManager(String topic, String dir/*, Level logLevel*/) {
//            LOG.setLevel(logLevel);
            this.kafkaStoredMessageDir = dir;
            this.topic = topic;

            String by = " by " + System.getProperty("user.name");
            try {
                by += "@" + InetAddress.getLocalHost().getHostName()+"(" +InetAddress.getLocalHost().getHostAddress() +")";
            }catch (Throwable t) {}
            File f = new File(kafkaStoredMessageDir);
            if(!f.exists()){
                boolean b = f.mkdirs();

                if(b) {
                    logInfo("Create dir done: ", f.toURI(), by);
//					LOG.info("Create dir done: " + f.toURI() + by);
                }else {
                    try {
                        logError("Celete file fail: ", f.toURI(), by);
//						LOG.error("Celete file fail: " + f.toURI() + by);
                    }catch (Throwable t) {
                        logError("Celete file fail: ", f.toURI(), by);
//						LOG.error("Celete file fail: " + f.toURI() + by);
                    }
                }
            }
            logInfo("Kafka store message dir: ", f.toURI(), by);
//			LOG.info("Kafka store message dir: " + f.toURI() + by);

            processMonitor = new ProcessMonitor(topic,dir + "___pid");

            initPolling();
        }

        public void initPolling(){
            new Thread(new Runnable() {
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(1000*2);
                        }catch (Throwable t) {
//							LOG.warn("Thread sleep fail", t);
                            logError(t,"Thread sleep fail");
                        }

                        String p = currOutputStreamPath;
                        OutputStream o = currentOutputStream;

                        try {
                            if(o != null) {
                                o.flush();
                            }
                        }catch (Throwable t) {
                            logError(t,"Current OutputStream(", p, ") flush fail");
//							LOG.warn("Current OutputStream("+p+") flush fail", t);
                        }

                        try {
                            deleteExpiredFiles();
                        }catch (Throwable t) {
                            logError(t, "Delete expired files fail");
//							LOG.warn("Delete expired files fail", t);
                        }

                    }
                }
            }).start();

        }

        //取出保存在文件的数据，成功回调后，将取出的数据文件后缀标记为.delete
        public void retrieve(final KafkaStoredMessageManagerRetrieveCallback kafkaStoredMessageAccessorRetrieveCallback) throws IOException {
            synchronizedCall(new Callback() {
                public void onCallback() {

//                    File dir = new File(kafkaStoredMessageDir);
//                    File[] fs = dir.listFiles(new FilenameFilter() {
//                        public boolean accept(File dir, String name) {
//                            return name.contains(pid()+".reading");
//                        }
//                    });
                    File[] fs = listFiles(kafkaStoredMessageDir, new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.contains(pid()+".reading");
                        }
                    });

                    for(int i = 0; i < fs.length; i++) {
                        File f = fs[i];
                        logInfo("Retrieve messages from reading file: ", f.getAbsolutePath());
//						LOG.info("Retrieve messages from reading file: " + f.getAbsolutePath());

                        List<KafkaStoredMessage> list = new ArrayList<KafkaStoredMessage>();

                        InputStream in = null;
                        StringBuilder buff = new StringBuilder();
                        String message = null;
                        try {
                            in = new FileInputStream(f);
                            List<String> lines = IOUtils.readLines(in);
                            for(String line : lines) {
                                buff.append(line).append("\r\n");
                            }

                            if(LOG.isDebugEnabled()) {
                                logDebug("Read plain reading file content:\n", buff.toString(), "\npath:\n", f.getAbsolutePath());
//								LOG.debug("Read plain reading file content:\n" + buff.toString() + "\npath:\n" + f.getAbsolutePath());
                            }

                            for(String o : buff.toString().split(RECORD_SEPARATOR)) {
                                message = o;
                                list.add(JSON.toBean(message, KafkaStoredMessage.class));
                            }

                            if(LOG.isDebugEnabled()) {
                                logDebug("Read parsed reading file content:\n", JSON.toJOSN(list), "\npath:\n", f.getAbsolutePath());
//								LOG.debug("Read parsed reading file content:\n" + JSON.toJOSN(list) + "\npath:\n" + f.getAbsolutePath());
                            }

                            kafkaStoredMessageAccessorRetrieveCallback.doCallback(f.getName().substring(0, f.getName().indexOf(FILE_NAME_TOPIC_SEPARATOR)), list);

                        } catch (Throwable e) {
                            if(message == null || "".equals(message.trim()) ) {
                                logError("Retrieve file to re-send data is empty");
//								LOG.error("Retrieve file to re-send data is empty");
                            }else {
                                logError(e,"Retrieve file to re-send error:");
//								LOG.error("Retrieve file to re-send error:", e);
                            }
                        }finally {
                            if(in != null) {
                                try{
                                    in.close();
                                }catch (Throwable e) {
//								e.printStackTrace();
                                    logError(e,"InputStream colse error");
//									LOG.error("InputStream colse error", e);
                                }
                            }
                        }

                        // Change to delete sign
                        logInfo("Try change to delete status:\n  from file: ", f.getAbsolutePath(), "\n  dest file: ",  f.getAbsolutePath().replaceAll(".reading", ".delete"));
//						LOG.info("Try change to delete status:\n  from file: " + f.getAbsolutePath() + "\n  dest file: " +  f.getAbsolutePath().replaceAll(".reading", ".delete"));
                        boolean b = f.renameTo(new File( f.getAbsolutePath().replaceAll(".reading", ".delete")));
                        if(b) {
                            logInfo("Change to delete status done !! dest file: ", f.getAbsolutePath().replaceAll(".reading", ".delete"));
//							LOG.info("Change to delete status done !! dest file: " + f.getAbsolutePath().replaceAll(".reading", ".delete"));
                        }else {
                            logInfo("Change to delete status fail !! original file: ", f);
//							LOG.info("Change to delete status fail !! original file: " + f);
                        }

                    }

//				for(File f : fs) {
//
//					LOG.info("Try change to reading status:\n  from file: " + f.getAbsolutePath() + "\n  dest file: " +  f.getAbsolutePath().replaceAll(".reading", ".delete"));
//					boolean b = f.renameTo(new File( f.getAbsolutePath().replaceAll(".reading", ".delete")));
//					if(b) {
//						LOG.info("Change to delete status done !! dest file: " + f.getAbsolutePath().replaceAll(".reading", ".delete"));
//					}else {
//						LOG.info("Change to delete status fail !! original file: " + f);
//					}
//				}
                }
            }, READ_LOCK);
        }

        // 添加数据，每隔指定分钟产生新的文件，将不再写入的文件后缀标记为 .reading
        public void store(final String topic, final KafkaStoredMessage storedKafkaMessage) {
            synchronizedCall(new Callback() {
                public void onCallback() {

                    try {
                        isCurrentOutputStreamWriting = true;
                        OutputStream out = generateOutputStream(topic);

                        String msg = JSON.toJOSN(storedKafkaMessage) + RECORD_SEPARATOR;

                        if(LOG.isDebugEnabled()) {
                            logDebug("Failed message stored:\n", msg, "path:\n", currOutputStreamPath);
//							LOG.debug("Failed message stored:\n" + message + "path:\n" + currOutputStreamPath);
                        }

                        out.write(msg.getBytes());
//					out.flush();
                        isCurrentOutputStreamWriting = false;
                    }catch (Exception e) {
                        LOG.error(e.getMessage());
                    }
                }
            }, WRITE_LOCK);
        }

        public static interface Callback{
            public void onCallback();
        }

        public static String pid(){
            return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        }

        public void synchronizedCall(Callback callback, Object lock){
            long st = System.currentTimeMillis();
            synchronized (lock) {
//			LOG.info("Synchronized call " + from+" wait time: " + (new Date().getTime() - currBatchSendTime)/1000F+"s");
                callback.onCallback();
            }
//		LOG.info("Synchronized call "+from+" use time: " + (new Date().getTime() - currBatchSendTime)/1000F+"s");
        }

        public synchronized OutputStream generateOutputStream(String topic) throws IOException {

            long sizeMB = 0L;
            if(currOutputStreamPath != null){
                sizeMB = new File(currOutputStreamPath).length()/1024/1024;
            }

            if(lastWritingSignCreateTime == null
                    || currentOutputStream == null
                    || System.currentTimeMillis() - lastWritingSignCreateTime > KafkaSender.WRITING_SIGN_EXPIRE_TIME_MS
                    || sizeMB > KafkaSender.MAX_FILE_STORE_SIZE_MB){

                closeCurrentOutputStream();

                String writingSign = DF.get().format(new Date());
                lastWritingSignCreateTime = System.currentTimeMillis();
                String f = kafkaStoredMessageDir + "/" + topic + FILE_NAME_TOPIC_SEPARATOR +  writingSign + PID_PREFIX + pid() + ".writing";
                logInfo("Writing new file: ", new File(f).toURI());
//				LOG.info("Writing new file: " + new File(f).toURI() );
                currentOutputStream = new BufferedOutputStream(new FileOutputStream(new File(f), true), 1024*1024*8);
                currOutputStreamPath = f;
            }

//		try{
//			Thread.sleep(10);
//		}catch (Exception e){
//			e.printStackTrace();
//		}
            return currentOutputStream;
        }

        public void closeCurrentOutputStream() throws IOException {
            //确保落盘
            if(currentOutputStream != null) {
                try{
                    currentOutputStream.flush();
                }catch (Throwable e) {
                    logError(e,"Flush current output stream fail");
//					LOG.warn("Flush current output stream fail", e);
                }
                finally {
                    try {
                        currentOutputStream.close();
                    }catch (Throwable t) {
                        logError(t, "Close current output stream fail");
//						LOG.warn("Close current output stream fail", t);
                    }

                    currentOutputStream = null;
                    currOutputStreamPath = null;
                }
            }
        }

        public void changeStatusToReadingIfWritingCompleted(){
            changeStatusToReadingIfWritingCompleted(false);
        }
        public void changeStatusToReadingIfWritingCompleted(final Boolean immediately){
            synchronizedCall(new Callback() {
                public void onCallback() {

                    LOG.warn("Try change status to reading if writing completed.");

                    processMonitor.listColsedProcess(new ProcessMonitor.CallBack() {
                        public void doCallback(final List<String> deadPids) {

                            if(deadPids.size() > 0) {
                                logWarn("Get dead-process pids: ", deadPids, ", Will change dead-process pid to current pid if exists '.delete' '.reading' or '.writing' file.");
//								LOG.warn("Get dead-process pids: " + deadPids + ", Will change dead-process pid to current pid if exists '.delete' '.reading' or '.writing' file.");
                            }

                            //Delete file (dead-process)
                            for(final String p : deadPids) {
//                                File[] fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
//                                    public boolean accept(File f) {
//                                        if(f.getName().contains(PID_PREFIX + p + ".delete")) {
//                                            return true;
//                                        }
//                                        return false;
//                                    }
//                                });

                                File[] fs = listFiles(kafkaStoredMessageDir, new FileFilter() {
                                    public boolean accept(File f) {
                                        if(f.getName().contains(PID_PREFIX + p + ".delete")) {
                                            return true;
                                        }
                                        return false;
                                    }
                                });

                                for(File f : fs) {
                                    boolean b = f.renameTo(new File(f.getAbsolutePath().replaceAll(PID_PREFIX + p +".delete", PID_PREFIX + pid() +".delete")));
                                    if(b) {
                                        logInfo("Change '.delete' file name from dead-pid '", p, "' to current-pid '", pid(), "' done ! from file: ", f);
//										LOG.info("Change '.delete' file name from dead-pid '"+p+"' to current-pid '"+pid()+"' done ! from file: " + f);
                                    }else {
                                        logInfo("Change '.delete' file name from dead-pid '", p, "' to current-pid '", pid(), "' fail ! from file: ", f);
//										LOG.info("Change '.delete' file name from dead-pid '"+p+"' to current-pid '"+pid()+"' fail ! from file: " + f);
                                    }
                                }
                            }

                            //Reading file (dead-process)
                            for(final String p : deadPids) {
//                                File[] fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
//                                    public boolean accept(File f) {
//                                        if(f.getName().contains(PID_PREFIX + p + ".reading")) {
//                                            return true;
//                                        }
//                                        return false;
//                                    }
//                                });
                                File[] fs = listFiles(kafkaStoredMessageDir, new FileFilter() {
                                    public boolean accept(File f) {
                                        if(f.getName().contains(PID_PREFIX + p + ".reading")) {
                                            return true;
                                        }
                                        return false;
                                    }
                                });

                                for(File f : fs) {
                                    boolean b = f.renameTo(new File(f.getAbsolutePath().replaceAll(PID_PREFIX + p +".reading", PID_PREFIX + pid() +".reading")));
                                    if(b) {
                                        logInfo("Change '.delete' file name from dead-pid '", p, "' to current-pid '", pid(), "' done ! from file: ", f);
//										LOG.info("Change '.delete' file name from dead-pid '"+p+"' to current-pid '"+pid()+"' done ! from file: " + f);
                                    }else {
                                        logInfo("Change '.delete' file name from dead-pid '", p, "' to current-pid '", pid(), "' fail ! from file: ", f);
//										LOG.info("Change '.delete' file name from dead-pid '"+p+"' to current-pid '"+pid()+"' fail ! from file: " + f);
                                    }
                                }
                            }

                            //Writing file
//                          File[] fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
                            File[] fs = listFiles(kafkaStoredMessageDir, new FileFilter() {
                                public boolean accept(File f) {
                                    String fn = f.getName();

                                    if(fn.contains(".writing")){

                                        String _fn = new StringBuffer(fn).reverse().toString();
                                        String pid = null;
                                        //文件名中提取pid
                                        int i = _fn.indexOf(new StringBuffer(".writing").reverse().toString()) + ".writing".length();
                                        int i2  = _fn.indexOf(PID_PREFIX,  i + 1);
                                        if(i >= 0 && i2 >= 0) {
                                            pid = new StringBuffer(_fn.substring(i, i2)).reverse().toString();
                                        }else {
                                            logError("Can not parse pid from file: ", f.getAbsolutePath());
                                            //throw new RuntimeException("Can not parse pid from file: " + f.getAbsolutePath());
                                        }

                                        //只读取自身的进程的维护的topic数据 和 其它已挂掉的进程topic数据
                                        if(!pid().equals(pid) && !deadPids.contains(pid)) {
                                            return false;
                                        }

                                        //Rename to current pid  (dead-process)
                                        if(deadPids.contains(pid)) {
                                            boolean b = f.renameTo(new File(f.getAbsolutePath().replaceAll(PID_PREFIX + pid +".writing", PID_PREFIX + pid() +".writing")));
                                            if(b) {
                                                logInfo("Change '.writing' file name from dead-pid '", pid, "' to current-pid '", pid(), "' done ! from file: ", f);
//												LOG.info("Change '.writing' file name from dead-pid '"+pid+"' to current-pid '"+pid()+"' done ! from file: " + f);
                                            }else {
                                                logInfo("Change '.writing' file name from dead-pid '", pid, "' to current-pid '", pid(), "' fail ! from file: ", f);
//												LOG.info("Change '.writing' file name from dead-pid '"+pid+"' to current-pid '"+pid()+"' fail ! from file: " + f);
                                            }
                                        }

                                        //排除当前正在写入的
                                        if(isCurrentOutputStreamWriting && currOutputStreamPath != null && currOutputStreamPath.contains(fn)){
                                            return false;
                                        }

                                        try {
                                            String signStr = fn.substring(fn.indexOf(FILE_NAME_TOPIC_SEPARATOR) + FILE_NAME_TOPIC_SEPARATOR.length(), fn.indexOf(PID_PREFIX));
                                            long sign = DF.get().parse(signStr).getTime();

                                            if(immediately || new Date().getTime() - sign >= KafkaSender.WRITING_SIGN_EXPIRE_TIME_MS) {

                                                if(deadPids.contains(pid)) {
                                                    logWarn("Will change dead-process pid '", pid, "' file '", fn, "' writing -> reading status");
//													LOG.warn("Will change dead-process pid '" + pid + "' file '" + fn + "' writing -> reading status");
                                                }

                                                closeCurrentOutputStream();
                                                return true;
                                            }
                                        }catch(Throwable e){
//									e.printStackTrace();
                                            LOG.error("Parse file name error, file: " + fn + ", kafkaStoredMessageDir: " + kafkaStoredMessageDir, e);
                                        }
                                    }
                                    return false;
                                }
                            });

                            for(File f : fs) {
                                logInfo("Try change to reading status:\n  from file: ", f.getAbsolutePath(), "\n  dest file: ",  f.getAbsolutePath().replaceAll(".writing", ".reading"));
//								LOG.info("Try change to reading status:\n  from file: " + f.getAbsolutePath() + "\n  dest file: " +  f.getAbsolutePath().replaceAll(".writing", ".reading"));
                                boolean b = f.renameTo(new File( f.getAbsolutePath().replaceAll(".writing", ".reading")));
                                if(b) {
                                    logInfo("Change to reading status done !! from file: ", f);
//									LOG.info("Change to reading status done !! from file: " + f);
                                }else {
                                    logInfo("Change to reading status fail !! original file: ", f);
//									LOG.info("Change to reading status fail !! original file: " + f);
                                }
                            }
                        }
                    });

                }
            },READ_LOCK);
        }

        public void deleteExpiredFiles(){

            // 按最多可保留的待删除文件数 判断是否删除
//            File[] fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
            File[] fs = listFiles(kafkaStoredMessageDir, new FileFilter() {
                public boolean accept(File f) {
                    String fn = f.getName();
                    if(fn.contains(pid()+".delete")){
                        return true;
                    }
                    return false;
                }
            } );

            if(fs.length > KafkaSender.MAX_DELETE_SIGN_KEEP_FILES) {
                Arrays.sort(fs, new Comparator<File>() {
                    public int compare(File a, File b) {
                        //文件名包含了时间，按时间降序
                        return -a.getName().compareTo(b.getName());
                    }
                });
            }

            for(int i = KafkaSender.MAX_DELETE_SIGN_KEEP_FILES - 1; i < fs.length; i++) {
                File f = fs[i];
                boolean b  = f.delete();
                if(b) {
                    logInfo("Delete file done(over max keep files: ", KafkaSender.MAX_DELETE_SIGN_KEEP_FILES+"): ", f.getAbsolutePath());
//					LOG.info("Delete file done(over max keep files: "+KafkaSender.MAX_DELETE_SIGN_KEEP_FILES+"): " + f.getAbsolutePath());
                }else {
                    logInfo("Delete file fail(over max keep files: ", KafkaSender.MAX_DELETE_SIGN_KEEP_FILES+"): ", f.getAbsolutePath());
//					LOG.info("Delete file fail(over max keep files: "+KafkaSender.MAX_DELETE_SIGN_KEEP_FILES+"): " + f.getAbsolutePath());
                }
            }

            // 按过期时间 判断是否删除
//            fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
            fs = listFiles(kafkaStoredMessageDir, new FileFilter() {
                public boolean accept(File f) {
                    String fn = f.getName();
                    if(fn.contains(pid()+".delete")){
                        try {
                            String signStr = fn.substring(fn.indexOf(FILE_NAME_TOPIC_SEPARATOR) + FILE_NAME_TOPIC_SEPARATOR.length(), fn.indexOf(PID_PREFIX));
                            long sign = DF.get().parse(signStr).getTime();

                            if(System.currentTimeMillis() - sign >= KafkaSender.MAX_DELETE_SIGN_EXPIRE_TIME_MS) {
                                return true;
                            }
                        }catch(Throwable e){
                            /*e.printStackTrace();*/
                            LOG.error(e.getMessage(), e);
                        }
                    }
                    return false;
                }
            });

            for(File f : fs) {
                logInfo("Deleting file: ", f.getAbsolutePath());
//				LOG.info("Deleting file: " + f.getAbsolutePath());
                boolean b = f.delete();
                if(b) {
                    logInfo("Delete file done(over max expire time: ", KafkaSender.MAX_DELETE_SIGN_EXPIRE_TIME_MS/1000/60/60+" hour): ", f.getAbsolutePath());
//					LOG.info("Delete file done(over max expire time: "+KafkaSender.MAX_DELETE_SIGN_EXPIRE_TIME_MS/1000/60/60+" hour): " + f.getAbsolutePath());
                }else {
                    logInfo("Delete file fail(over max expire time: ", KafkaSender.MAX_DELETE_SIGN_EXPIRE_TIME_MS/1000/60/60+" hour): ", f.getAbsolutePath());
//					LOG.info("Delete file fail(over max expire time: "+KafkaSender.MAX_DELETE_SIGN_EXPIRE_TIME_MS/1000/60/60+" hour): " + f.getAbsolutePath());
                }
            }
        }

        public static class ProcessMonitor{

//	public static final String BASE_DIR = "C:\\Users\\Administrator\\Documents\\Tencent Files\\2060878177\\FileRecv\\新建文件夹\\s";

            private String baseDir;
            private String topic;
            public ProcessMonitor(String topic, String baseDir){
                this.baseDir = baseDir;
                this.topic = topic;
                File dir = new File(baseDir);
                if(!dir.exists()) {
                    dir.mkdirs();
                }

                initProcess();
            }

            private void logError(Throwable throwable, Object... msg){LOG.error(logMsg(topic, msg), throwable);}
            private void logError(Object... msg){LOG.error(logMsg(topic, msg));}
            private void logInfo(Object... msg){LOG.info(logMsg(topic, msg));}
            private void logWarn(Object... msg){LOG.warn(logMsg(topic, msg));}
            private void logDebug(Object... msg){LOG.info(logMsg(topic, msg));}

            public interface CallBack {
                void doCallback(List<String> pids);
            }

            private void initProcess(){

                new Thread(new Runnable() {
                    public void run() {


                        try {

                            FileLock lock = null;

                            File f = new File(baseDir + "/" + pid()+".pid");

                            if(f.exists()) {
                                throw new RuntimeException("initProcess() can only be run once!");
                            }

                            FileOutputStream out = new FileOutputStream(f);

                            FileChannel fc = out.getChannel();
                            lock = fc.lock();
                            if(lock != null) {
                                logInfo("Get init lock success via thread[", pid(), ":", Thread.currentThread().getId(), "], file: ", f.getAbsolutePath(), ", lock: ", lock);
//							LOG.warn("Get init lock success via thread["+pid()+":" +Thread.currentThread().getId()+"], file: "+f.getAbsolutePath() + ", lock: " + lock);
                            }

                            fc.write(ByteBuffer.wrap("inited".getBytes()));
                            fc.force(true);

//						Keep lock active !!
                            while (true) {
                                try {
                                    Thread.sleep(2000);
                                }catch (Throwable t){
                                    t.printStackTrace();
                                }
                            }

                        }catch (Throwable t) {
                            logError(t,"Call initProcess() fail:");
//						LOG.warn("Call initProcess() fail:", t);
                            throw new RuntimeException(t.getMessage(),t);
                        }
                    }

                }).start();

            }

//		public static String pid(){
//			return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
//		}

            public  synchronized  void listColsedProcess(CallBack callback){
                List<String> result = new ArrayList<String>();
//                File[] fs = new File(baseDir).listFiles();
                File[] fs = listFiles(baseDir);
                Map<FileChannel, FileLock> locks = new HashMap<FileChannel, FileLock>();
                Map<FileChannel, FileLock> locks2 = new HashMap<FileChannel, FileLock>();

                for(File f : fs) {
                    FileChannel fc = null;
                    FileLock lock = null;
                    try {
                        RandomAccessFile af = new RandomAccessFile(f, "rw");

                        fc = af.getChannel();

//                fc = new FileOutputStream(f, true).getChannel();
                        lock = fc.tryLock();
                        if(lock != null) {
                            logWarn("Get locks via pid: ", pid(), " thread: ", Thread.currentThread().getId(), " success", ", file: ", f.getAbsolutePath(), ", lock: ", lock);
//							LOG.warn("Get list lock success via thread["+pid()+":"+Thread.currentThread().getId()+"], file: "+f.getAbsolutePath() + ", lock: " + lock);
                        }

                        //如果能成功读取说明对应的进程已经关闭
                        ByteBuffer dst = ByteBuffer.wrap(new byte[1024]);
                        StringBuilder buff = new StringBuilder();

                        /*读入到buffer*/
                        int b = fc.read(dst);
                        while(b!=-1){
                            /*设置读*/
                            dst.flip();
                            /*开始读取*/
                            while(dst.hasRemaining())
                            {
                                buff.append((char)dst.get());
                            }
                            dst.clear();
                            b = fc.read(dst);
                        }

//                		System.out.println(buff);
                        if("inited".equals(buff.toString())) {
                            String n = f.getName();
                            result.add(n.substring(0, n.indexOf(".")));
                            locks2.put(fc, lock);
                        }else {
                            locks.put(fc, lock);
                        }

                    }
                    catch (Exception e) {
                        String fPid = f.getName().replace(".pid", "");

                        if(pid().equals(fPid)) continue;

                        logWarn("Get locks via pid: ", pid(), " thread: ", Thread.currentThread().getId(), " error", ", file: ", f.getAbsolutePath(), ", lock: ", lock, ", error: ", getExceptionStackTrace(e));
//                        logWarn("Get locks fail via thread[", pid(), ":", Thread.currentThread().getId(), "], file: ", f.getAbsolutePath(), ", because: OverlappingFileLockException");
//						LOG.warn("Get list lock fail via thread["+pid()+":"+Thread.currentThread().getId()+"], file: "+f.getAbsolutePath()+", because: OverlappingFileLockException");
                    }
//                    catch (Exception e) {
////                        logWarn(f.getAbsolutePath(), ": ", e.getMessage());
////						LOG.warn(f.getAbsolutePath() + ": " + e.getMessage());
//                    }
                    finally {

                    }
                }

//				System.out.println("RESULT::" + result);
                callback.doCallback(result);


                for(Map.Entry<FileChannel, FileLock> e: locks2.entrySet()) {
                    try {
                        e.getKey().write(ByteBuffer.wrap(("\r\nclosed by pid " + pid()).getBytes()));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    if(e.getValue() != null) {
                        try {
                            e.getValue().release();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                    if(e.getKey() != null) {
                        try {
                            e.getKey().close();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                }
                for(Map.Entry<FileChannel, FileLock> e: locks.entrySet()) {
                    if(e.getValue() != null) {
                        try {
                            e.getValue().release();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                    if(e.getKey() != null) {
                        try {
                            e.getKey().close();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                }


            }
        }
    }

    public static File[] listFiles(String fileDir){
        File[] fs =  new File(fileDir).listFiles();
        return fs == null ? fs = new File[0] : fs;
    }
    public static File[] listFiles(String fileDir, FilenameFilter filter){
        File[] fs =  new File(fileDir).listFiles(filter);
        return fs == null ? fs = new File[0] : fs;
    }
    public static File[] listFiles(String fileDir, FileFilter filter){
        File[] fs =  new File(fileDir).listFiles(filter);
        return fs == null ? fs = new File[0] : fs;
    }

    public static String getExceptionStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }
}

class JSON {

    private static ObjectMapper objectMapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, false).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    public static String toJOSN(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toBean(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toBean(String json, TypeReference<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}



