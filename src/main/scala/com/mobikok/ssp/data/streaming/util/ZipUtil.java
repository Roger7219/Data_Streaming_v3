package com.mobikok.ssp.data.streaming.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// 将一个字符串按照zip方式压缩和解压缩
public class ZipUtil {

//    // 压缩
//    public static String compress(String str) throws IOException {
//        if (str == null || str.length() == 0) {
//            return str;
//        }
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        GZIPOutputStream gzip = new GZIPOutputStream(out);
//        gzip.write(str.getBytes());
//        gzip.close();
//        return out.toString("ISO-8859-1");
//    }
//
//    // 解压缩
//    public static String uncompress(String str) throws IOException {
//        if (str == null || str.length() == 0) {
//            return str;
//        }
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ByteArrayInputStream in = new ByteArrayInputStream(str
//                .getBytes("ISO-8859-1"));
//        GZIPInputStream gunzip = new GZIPInputStream(in);
//        byte[] buffer = new byte[256];
//        int n;
//        while ((n = gunzip.read(buffer)) >= 0) {
//            out.write(buffer, 0, n);
//        }
//// toString()使用平台默认编码，也可以显式的指定如toString("GBK")
//        return out.toString();
//    }
//
//    // 测试方法
//    public static void main(String[] args) throws IOException {
//        System.out.println(ZipUtil.compressToBase64("中国China"));
//        System.out.println(ZipUtil.decompressFromBase64(ZipUtil.compressToBase64("中国China")));
//    }

    public static String compressToBase64(String string){
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream(string.length());
            GZIPOutputStream gos = new GZIPOutputStream(os);
            gos.write(string.getBytes());
            gos.close();
            byte[] compressed = os.toByteArray();
            os.close();

            String result = org.apache.commons.codec.binary.Base64.encodeBase64String(compressed);
//            String result = Base64.encodeToString(compressed, Base64.DEFAULT);
            return result;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


    public static String decompressFromBase64(String textToDecode){
        //String textToDecode = "H4sIAAAAAAAAAPNIzcnJBwCCidH3BQAAAA==\n";
        try {

            byte[] compressed = org.apache.commons.codec.binary.Base64.decodeBase64(textToDecode);
//            byte[] compressed = Base64.decode(textToDecode, Base64.DEFAULT);

            final int BUFFER_SIZE = 32;
            ByteArrayInputStream inputStream = new ByteArrayInputStream(compressed);
            GZIPInputStream gis  = new GZIPInputStream(inputStream, BUFFER_SIZE);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] data = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = gis.read(data)) != -1) {
                baos.write(data, 0, bytesRead);
            }
            return baos.toString("UTF-8");
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}