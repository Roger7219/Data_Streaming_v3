package com.mobikok.ssp.data.streaming;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mobikok.ssp.data.streaming.util.OM;
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
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaUtil {

	private  static  SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS");

	public static void main(String[] args) throws InterruptedException, IOException {

//		File f = new File("C:\\Users\\kairenlo\\Desktop\\kafka_stored_message\\topic_KafkaUtilTest_01$20171106012544633.9884.delete");
//
//		List<String> list = IOUtils.readLines(new FileInputStream(f));
//
//		StringBuilder buff = new StringBuilder();
//		for (String s : list) {
//			buff.append(s);//.append("\n");
//		}
//		String RECORD_SEPARATOR = "\n^\n";
//		System.out.println(buff.toString().split(RECORD_SEPARATOR)[0]);


		while (true) {
			send("topic_KafkaUtilTest_01", "axxxxxxxxxxxxxxxxxxxxxxxxxxx_" +  DF.format( new Date()));
			Thread.sleep(1000*10*1);
		}
	}

	private static Logger logger = LoggerFactory.getLogger(KafkaUtil.class);
	
	private static boolean isClose = false;

	private static KafkaProducer<String, String> PRODUCER;

	private static String kafkaStoredMessageDir = "C:\\Users\\kairenlo\\Desktop\\kafka_stored_message\\";

	private static KafkaStoredMessageManager kafkaStoredMessageManager = new KafkaStoredMessageManager(kafkaStoredMessageDir);


	public static void send(String topic, String... messages) {
		sendWithKey(topic, null, messages);
	}
	
	static{
		createIfInvalidProducer();

		new Thread(new Runnable() {

			public void run() {

				try {
					Thread.sleep(1000*5);
					while(true) {
						kafkaStoredMessageManager.changeStatusToReadingIfWritingCompleted();

						//获取并移出元素
						kafkaStoredMessageManager.retrieve(new KafkaStoredMessageManagerRetrieveCallback(){
							public void doCallback(String topic, List<KafkaStoredMessage> kafkaStoredMessages) {
								logger.warn("Retrieve re-send to kafka starting, size: " + kafkaStoredMessages.size());
								String[] arr = new String[kafkaStoredMessages.size()];
								for(int i = 0; i < kafkaStoredMessages.size(); i++) {
									arr[i] = kafkaStoredMessages.get(i).getMsg();
								}
								KafkaUtil.send(topic, arr);
								logger.warn("Retrieve re-send to kafka done, size: " + kafkaStoredMessages.size());
							}
						});

						Thread.sleep(1000*60*1);
					}

				}catch(Throwable t) {
					t.printStackTrace();
				}
			}
		}).start();
	}
	
	public static void createIfInvalidProducer() {
		if(PRODUCER == null) {
			PRODUCER = new KafkaProducer<String, String>(getProducerConfig());
		}
		if(isClose) {
			PRODUCER = new KafkaProducer<String, String>(getProducerConfig());
		}
	}
	

	public static void sendWithKey(String topic, String key, String... messages) {
		try {
			createIfInvalidProducer();

			int size = messages.length;
			if (Constants.KAFKA_PRODUCER_IS_ASYNC) {
				for(String m : messages){
					PRODUCER.send(new ProducerRecord<String, String>(topic, key, m), new MessageProduceCallback(size, System.currentTimeMillis(), topic, key, m));
				}
			}else {
				for(String m : messages){
					PRODUCER.send(new ProducerRecord<String, String>(topic, key, m)).get();
				}
			}
			PRODUCER.flush();
		}catch(Exception e) {
			logger.error("Exception when sending Kafka messages!!!", e);
			PRODUCER.close();
		}
	}
	
	public static void closeProducer(){
		PRODUCER.close();
		isClose = true;
		//PRODUCER = new KafkaProducer<String, String>(getProducerConfig());
	}

	private static Properties getProducerConfig() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.KAFKA_CLIENT_ID);
		props.put(ProducerConfig.ACKS_CONFIG, Constants.KAFKA_ACKS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.KAFKA_KEY_SERIALIZER);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,Constants.KAFKA_VALUE_SERIALIZER);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000*10);


		return props;
	}
	
	// ==============================================================================================

	public static interface KafkaStoredMessageManagerRetrieveCallback {
		void doCallback(String topic, List<KafkaStoredMessage> kafkaStoredMessages);
		
	}
	public static class MessageProduceCallback implements org.apache.kafka.clients.producer.Callback {
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
				logger.warn("Kafka send fail !! msg: \n"+ OMX.toJOSN(storedKafkaMessage) +" error: ", e);
				//加入元素
				try {
					kafkaStoredMessageManager.store(topic, storedKafkaMessage);
				} catch (Exception ex) {
					logger.warn("Store fail:", ex);
				}
				
			}else {
				if (recordMetadata != null && logger.isDebugEnabled()) {
					long elapsedTime = System.currentTimeMillis() - startTime;
					logger.debug("Kafka send succeeded, message : " + storedKafkaMessage.getMsg()
							+ ", partition id: "
							+ recordMetadata.partition() + ", elapsedTime: "
							+ elapsedTime);
				}	
			}
			
		}
		
	}

	public static class KafkaStoredMessage {
		private String topic;
		private String msg;

		public KafkaStoredMessage(){}

		public KafkaStoredMessage(String topic, String msg) {
			this.topic = topic;
			this.msg = msg;
		}
		public String getTopic() {
			return topic;
		}
		public String getMsg() {
			return msg;
		}
		public void setTopic(String topic) {
			this.topic = topic;
		}
		public void setMsg(String msg) {
			this.msg = msg;
		}
	}

}

class KafkaStoredMessageManager {
	private String kafkaStoredMessageDir;

	private static Logger logger = LoggerFactory.getLogger(KafkaStoredMessageManager.class);

	private SimpleDateFormat DF = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	private static String FILE_NAME_TOPIC_SEPARATOR = "$";
	private Long lastWritingSignCreateTime = null;
	volatile private OutputStream currentOutputStream = null;

	public KafkaStoredMessageManager(String dir) {
		this.kafkaStoredMessageDir = dir;
    }

	//取出保存在文件的数据，成功回调后，将取出的数据文件后缀标记为.delete
	public void retrieve(final KafkaUtil.KafkaStoredMessageManagerRetrieveCallback kafkaStoredMessageAccessorRetrieveCallback) throws IOException {
		synchronizedCall(new Callback() {
			public void onCallback() {

				File dir = new File(kafkaStoredMessageDir);
				File[] fs = dir.listFiles(new FilenameFilter() {
					public boolean accept(File dir, String name) {
						return name.contains(".reading");
					}
				});

				List<String> fns = new ArrayList<String>();
				for (File f : fs) {
					fns.add(f.getAbsolutePath());
				}
				logger.warn("Read all reading files:\n"+ Arrays.deepToString(fns.toArray()));

				List<KafkaUtil.KafkaStoredMessage> list = new ArrayList<KafkaUtil.KafkaStoredMessage>();
				for(int i = 0; i < fs.length; i++) {

					InputStream in = null;
					StringBuilder buff = new StringBuilder();
					try {
						File f = fs[i];
						in = new FileInputStream(f);
						List<String> lines = IOUtils.readLines(in);
						for(String line : lines) {
							buff.append(line).append("\n");
						}

						for(String o : buff.toString().split(RECORD_SEPARATOR)) {
							list.add(OMX.toBean(o, KafkaUtil.KafkaStoredMessage.class));
						}

						logger.warn("Read parsed reading file content:\n" + OMX.toJOSN(list));

						kafkaStoredMessageAccessorRetrieveCallback.doCallback(f.getName().substring(0, f.getName().indexOf(FILE_NAME_TOPIC_SEPARATOR)), list);

					} catch (IOException e) {
						e.printStackTrace();
					}finally {
						if(in != null) {
							try{
								in.close();
							}catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}

				for(File f : fs) {
					f.renameTo(new File( f.getAbsolutePath().replaceAll(".reading", ".delete")));
				}
			}
		});
	}

	// 添加数据，每隔15分钟产生新的文件，将不再写入的文件后缀标记为 .reading
	public void store(final String topic, final KafkaUtil.KafkaStoredMessage storedKafkaMessage) {

		synchronizedCall(new Callback() {
			public void onCallback() {

				try {
					OutputStream out = generateOutputStream(topic);
					String msg = OM.toJOSN(storedKafkaMessage) + RECORD_SEPARATOR;
					logger.warn("Store send failed msg:\n" + msg + "path:\n" + outputStreamPath);
					out.write(msg.getBytes());
					out.flush();

				}catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});

	}

	private int CURRENT_WRITING_SIGN_TIMEOUT_MS = 1000*60*1;

	private String PID_PREFIX = ".";
	private static String RECORD_SEPARATOR = "\n^\n";

	private static Object SYNC_LOCK = new Object();

	private String outputStreamPath = null;


	public static interface Callback{
		public void onCallback();
	}

	public void synchronizedCall(Callback callback){
		synchronized (SYNC_LOCK) {
			callback.onCallback();
		}
	}

	public OutputStream generateOutputStream(String topic) throws IOException {

		Date now = new Date();

		if(lastWritingSignCreateTime == null || currentOutputStream == null || now.getTime() - lastWritingSignCreateTime > CURRENT_WRITING_SIGN_TIMEOUT_MS){

			closeCurrentOutputStream();

			String writingSign = DF.format(now);
			lastWritingSignCreateTime = now.getTime();
			String f = kafkaStoredMessageDir +  topic + FILE_NAME_TOPIC_SEPARATOR +  writingSign + PID_PREFIX + pid() + ".writing";
			logger.warn("Writing file: " + f);
			currentOutputStream = new FileOutputStream(new File(f), true);
			outputStreamPath = f;
		}

		try{
			Thread.sleep(10);
		}catch (Exception e){
			e.printStackTrace();
		}
		return currentOutputStream;
	}

	public void closeCurrentOutputStream() throws IOException {
		//确保落盘
		if(currentOutputStream != null) {
			try{
				currentOutputStream.flush();
				currentOutputStream.close();
			}catch (Exception e) {
				logger.warn("Close current writing output stream fail", e);
			}
			finally {
				currentOutputStream = null;
				outputStreamPath = null;
			}


		}
	}
	public String pid(){
		return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}

	public void changeStatusToReadingIfWritingCompleted(){
		synchronizedCall(new Callback() {
			public void onCallback() {
				File[] fs = new File(kafkaStoredMessageDir).listFiles(new FileFilter() {
					public boolean accept(File f) {
						String fn = f.getName();
						if(fn.contains(".writing")){
							try {
								String signStr = fn.substring(fn.indexOf(FILE_NAME_TOPIC_SEPARATOR) + 1, fn.indexOf(PID_PREFIX));
								long sign = DF.parse(signStr).getTime();

								if(new Date().getTime() - sign >= CURRENT_WRITING_SIGN_TIMEOUT_MS) {
									closeCurrentOutputStream();
									return true;
								}
							}catch(Exception e){e.printStackTrace();}
						}
						return false;
					}
				});

				for(File f : fs) {
					logger.warn("Try change to reading status:\nfrom file: " + f.getAbsolutePath() + "\ndest file: " +  f.getAbsolutePath().replaceAll(".writing", ".reading"));
					boolean b = f.renameTo(new File( f.getAbsolutePath().replaceAll(".writing", ".reading")));
					if(b) {
						logger.warn("Change to reading status done !! from file: " + f);
					}else {
						logger.warn("Change to reading status fail !! original file: " + f);
					}
				}
			}
		});
	}
};

class OMX {

	private static ObjectMapper objectMapper = new ObjectMapper().configure(
			SerializationFeature.INDENT_OUTPUT, true).disable(
			DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

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

	public static <T> T convert(Object o, TypeReference<T> clazz) {
		try {
			return objectMapper.convertValue(o, clazz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T convert(Object o, Class<T> clazz) {
		try {
			return objectMapper.convertValue(o, clazz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}


class Constants {
	
	public static final boolean KAFKA_PRODUCER_IS_ASYNC = true;
//	public static final String KAFKA_BOOTSTRAP_SERVERS = "node14:6667,node15:6667,node17:6667";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "node14:9292";
	public static final String KAFKA_CLIENT_ID = "ssp_backup_new_producer2";
	public static final String KAFKA_ACKS = "-1"; 
	public static final String KAFKA_KEY_SERIALIZER = StringSerializer.class.getName();
	public static final String KAFKA_VALUE_SERIALIZER = StringSerializer.class.getName();
	
}

