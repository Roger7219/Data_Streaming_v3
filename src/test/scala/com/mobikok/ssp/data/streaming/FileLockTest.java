package com.mobikok.ssp.data.streaming;


import com.mobikok.ssp.data.streaming.util.KafkaSender;
import com.sun.corba.se.impl.activation.ProcessMonitorThread;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;

/**
 * Created by Administrator on 2017/12/13.
 */
public class FileLockTest {


    public static void main(String[] args) throws Exception {

//        final KafkaSender.KafkaStoredMessageManager.ProcessMonitor m= new KafkaSender.KafkaStoredMessageManager.ProcessMonitor("C:\\home\\kok\\app\\ssp\\ssp_backup\\ROOT\\kafka_stored_message");
//
//
//        new Thread(new Runnable() {
//            public void run() {
//                while (true) {
//
//                    try {
//                        Thread.sleep(1000*5);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    m.listColsedProcess(new KafkaSender.KafkaStoredMessageManager.ProcessMonitor.CallBack() {
//                    public void doCallback(List<String> pids) {
//                        System.out.println("result:: " + pids);
//                    }
//                });
//                }
//            }
//        }).start();


        KafkaSender.instance("sss");

//        System.out.println(pid() + ": curr");
//        final ProcessMonitor pm = new ProcessMonitor("C:\\home\\kok\\app\\ssp\\ssp_backup\\ROOT\\kafka_stored_message2");
//        new Thread(new Runnable() {
//            public void run() {
//                pm.listColsedProcess(new ProcessMonitor.CallBack() {
//                    public void doCallback(List<String> pids) {
//                        System.out.println("pids:" + pids);
//                    }
//                });
//            }
//        }).start();

//            initProcess();
//
//            listColsedProcess();
    //        final File f = new File("C:\\Users\\Administrator\\Documents\\Tencent Files\\2060878177\\FileRecv\\新建文件夹\\" + pid());
//        if(!f.exists()) {
//            f.createNewFile();
//        }
//
//        FileOutputStream outStream = new FileOutputStream(f);
//        FileChannel channel = outStream.getChannel();
//
//        channel.lock();
//        System.out.println("Get the Lock!");
//
//        new Thread(new Runnable() {
//            public void run() {
//                while (true) {
//
//                    try {
//                        try {
//                            Thread.sleep(1000);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        File f2 = new File(f.getAbsolutePath());
//
//                        String f = FileUtils.readFileToString(f2);
//
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//
//                }
//            }
//        }).start();
        System.out.println("end");
        Thread.sleep(10000000L);



    }

    public static final String BASE_DIR = "C:\\Users\\Administrator\\Documents\\Tencent Files\\2060878177\\FileRecv\\新建文件夹\\s";

    public static void initProcess(){
        try {
            File dir = new File(BASE_DIR);
            if(!dir.exists()) {
                dir.mkdirs();
            }

            File f = new File(BASE_DIR + "/" + pid()+".pid");

            if(f.exists()) {
                throw new RuntimeException("initProcess() can only be run once!");
            }

            FileOutputStream out = new FileOutputStream(f);

            FileChannel fc = out.getChannel();
            fc.lock();

            fc.write(ByteBuffer.wrap("inited".getBytes()));
//            fc.force(true);
            out.flush();
//            fc.close();
            System.out.println("xxxxxxxxxxxx");
        }catch (Throwable t) {
            throw new RuntimeException(t.getMessage(),t);
        }

    }

    static Map<FileChannel, FileLock> locks = new HashMap<FileChannel, FileLock>();

    public static List<String> listColsedProcess(){
        List<String> result = new ArrayList<String>();
        File[] fs = new File(BASE_DIR).listFiles();

        for(File f : fs) {
            FileChannel fc = null;
            FileLock lock = null;
            try {
                RandomAccessFile af =new RandomAccessFile(f, "rw");

                fc = af.getChannel();

//                fc = new FileOutputStream(f, true).getChannel();
                lock = fc.tryLock();

                System.out.println("LOCK " + lock);
                //如果能成功读取说明对应的进程已经关闭
                ByteBuffer dst = ByteBuffer.wrap(new byte[1024]);
                StringBuilder buff = new StringBuilder();

                /*读入到buffer*/
                int b = fc.read(dst);
                while(b!=-1)
                {
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

//                System.out.println(buff);
                if("inited".equals(buff.toString())) {
                    String n = f.getName();
                    result.add(n.substring(0, n.indexOf(".")));
                    locks.put(fc, lock);
                }

            }
            catch (OverlappingFileLockException e) {e.printStackTrace();}
            catch (Exception e) {e.printStackTrace();}
            finally {

            }
        }
        System.out.println("RESULT::" + result);

        for(Map.Entry<FileChannel, FileLock> e: locks.entrySet()) {
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

        return result;

    }

    public static String pid(){
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }
}


//
//
//class ProcessMonitor{
//
////	public static final String BASE_DIR = "C:\\Users\\Administrator\\Documents\\Tencent Files\\2060878177\\FileRecv\\新建文件夹\\s";
//
//    private String baseDir;
//    public ProcessMonitor(String baseDir){
//        this.baseDir = baseDir;
//        initProcess();
//    }
//
//    public static interface CallBack {
//        void doCallback(List<String> pids);
//    }
//
//    private void initProcess(){
//        try {
//            File dir = new File(baseDir);
//            if(!dir.exists()) {
//                dir.mkdirs();
//            }
//
//            File f = new File(baseDir + "/" + pid()+".pid");
//
//            if(f.exists()) {
//                throw new RuntimeException("initProcess() can only be run once!");
//            }
//
//            FileOutputStream out = new FileOutputStream(f);
//
//            FileChannel fc = out.getChannel();
//            fc.lock();
//
//            fc.write(ByteBuffer.wrap("inited".getBytes()));
//            fc.force(true);
//        }catch (Throwable t) {
//            throw new RuntimeException(t.getMessage(),t);
//        }
//
//    }
//
//    public static String pid(){
//        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
//    }
//
//    public void listColsedProcess(ProcessMonitor.CallBack callback){
//        List<String> result = new ArrayList<String>();
//        File[] fs = new File(baseDir).listFiles();
//        Map<FileChannel, FileLock> locks = new HashMap<FileChannel, FileLock>();
//
//        for(File f : fs) {
//            FileChannel fc = null;
//            FileLock lock = null;
//            try {
//                RandomAccessFile af =new RandomAccessFile(f, "rw");
//
//                fc = af.getChannel();
//
////                fc = new FileOutputStream(f, true).getChannel();
//                lock = fc.tryLock();
//
//                System.out.println("LOCK " + lock);
//                //如果能成功读取说明对应的进程已经关闭
//                ByteBuffer dst = ByteBuffer.wrap(new byte[1024]);
//                StringBuilder buff = new StringBuilder();
//
//                /*读入到buffer*/
//                int b = fc.read(dst);
//                while(b!=-1)
//                {
//                    /*设置读*/
//                    dst.flip();
//                    /*开始读取*/
//                    while(dst.hasRemaining())
//                    {
//                        buff.append((char)dst.get());
//                    }
//                    dst.clear();
//                    b = fc.read(dst);
//                }
//
////                System.out.println(buff);
//                if("inited".equals(buff.toString())) {
//                    String n = f.getName();
//                    result.add(n.substring(0, n.indexOf(".")));
//                    locks.put(fc, lock);
//                }
//
//            }
//            catch (OverlappingFileLockException e) {e.printStackTrace();}
//            catch (Exception e) {e.printStackTrace();}
//            finally {
//
//            }
//        }
//        callback.doCallback(result);
//        System.out.println("RESULT::" + result);
//
//        for(Map.Entry<FileChannel, FileLock> e: locks.entrySet()) {
//            try {
//                e.getKey().write(ByteBuffer.wrap(("\r\nclosed by pid " + pid()).getBytes()));
//            } catch (IOException e1) {
//                e1.printStackTrace();
//            }
//            if(e.getValue() != null) {
//                try {
//                    e.getValue().release();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
//            }
//            if(e.getKey() != null) {
//                try {
//                    e.getKey().close();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
//            }
//        }
//
//    }
//}