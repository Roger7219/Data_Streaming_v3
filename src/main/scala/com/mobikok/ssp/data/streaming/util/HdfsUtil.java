package com.mobikok.ssp.data.streaming.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2018/8/1 0001.
 */

public class HdfsUtil {

    private Logger LOG = Logger.getLogger(HdfsUtil.class);
    private static String uri = "hdfs://master:8020";

    /**
     * 设置hadoop HDFS 初始化配置方法
     * @throws IOException
     */
    public static FileSystem init(){
        Configuration config=new Configuration();
        config.set("fs.defaultFS", uri);
        FileSystem fs = null;
        try{
            fs=FileSystem.get(new URI(uri), config, "hdfs");
        }catch(Exception e){
            throw new RuntimeException("初始化异常");
        }
        return fs;
    }


    /**
     * make a new dir in the hdfs
     *
     * @param dir the dir may like '/pluggable/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException something wrong happends when operating files
     */
    public  boolean mkdir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
//        dir = uri + dir;
        FileSystem fs = init();
        if (!fs.exists(new Path(dir))) {
            fs.mkdirs(new Path(dir));
        }

        fs.close();
        return true;
    }

    /**
     * delete a dir in the hdfs.
     * if dir not exists, it will throw FileNotFoundException
     *
     * @param path the dir may like '/pluggable/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException something wrong happends when operating files
     *
     */
    public boolean delete(String path, Boolean recursive) throws IOException {
        if (StringUtils.isBlank(path)) {
            return false;
        }

        LOG.warn("delete start!!");
        FileSystem fs = init();

        fs.delete(new Path(path), recursive);
        if(recursive){
            LOG.warn("delete path: " + path);
        }else {
            LOG.warn("delete file: " + path);
        }

        fs.close();
        LOG.warn("delete end!!");
        return true;
    }

    public boolean rename(String srcFile, String dstFile) throws Exception {
        if (StringUtils.isBlank(srcFile) || StringUtils.isBlank(dstFile)) {
            return false;
        }

        LOG.warn("rename start!");
        FileSystem fs = init();

        fs.rename(new Path(srcFile), new Path(dstFile));

        fs.close();
        LOG.warn("rename end!");
        return true;
    }

    public boolean move(String src, String dst) throws Exception {
        if (StringUtils.isBlank(src) || StringUtils.isBlank(dst)) {
            return false;
        }

        LOG.warn("move start!!");
        FileSystem fs = init();

        List<String> files = getFilePaths(src);

        for (String file: files) {

            String[] splits = file.split("/");
            String fileName = splits[splits.length - 1];


            fs.rename(new Path(file), new Path(dst + "/" + fileName));
            LOG.warn("move file from: " + file + " to: " + dst + "/" + fileName);
        }

//        fs.rename(new Path(src), new Path(dst));
        fs.close();
        LOG.warn("move end!!");
        return true;
    }

    public boolean renameMove(String suffix, String src, String dst) throws Exception {
        if (StringUtils.isBlank(src) || StringUtils.isBlank(dst)) {
            return false;
        }

        LOG.warn("renameMove start!");
        FileSystem fs = init();

        List<String> srcFilePaths = getFilePaths(src);

        for (String file: srcFilePaths) {

            String newFile = file + suffix;
            //重命名
            fs.rename(new Path(file), new Path(newFile));

            LOG.warn("renameMove: rename from " + file + " to" + newFile);
            //移动

            String[] splits = newFile.split("/");
            String fileName = splits[splits.length - 1];

            fs.rename(new Path(newFile), new Path(dst + "/" + fileName));

            LOG.warn("renameMove: move from " + newFile + " to" + dst);
        }

       /* for (FileStatus stat: stats) {

            String newFile = stat.getPath().toString() + suffix;
            //重命名
            fs.rename(stat.getPath(), new Path(newFile));
            //移动
            fs.rename(new Path(newFile), new Path(dst));
        }*/

        fs.close();
        LOG.warn("renameMove end!");
        return true;
    }

    /**
     * recursive get the full-path of file from dir
     * @param dir
     * @throws Exception
     */
    public List<String> getFilePaths(String dir) throws Exception {

        LOG.warn("getFilePaths start!! ");
        FileSystem fs = init();

        List<String> filePaths = new ArrayList<String>();

        FileStatus[] globStatus = fs.listStatus(new Path(dir));

        Path[] stat2Paths = FileUtil.stat2Paths(globStatus);

        for (Path path: stat2Paths) {
            if (fs.isDirectory(path)){
                LOG.warn("getFilePaths  isDirectory: " + path);
                getFilePaths(path.toString());
            }else if(fs.isFile(path)){
                LOG.warn("getFilePaths isFile, add file: " + path.toString());
                filePaths.add(path.toString());
            }

        }

        LOG.warn("getFilePaths end!! ");
        return filePaths;

    }

    /**
     * list files/directories/links names under a directory, not include embed
     * objects
     *
     * @param dir a folder path may like '/pluggable/testdir'
     * @return List<String> list of file names
     * @throws IOException file io exception
     */
    public List<String> listAll(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }

        FileSystem fs = init();
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isDirectory()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isSymlink()) {
                // is s symlink in linux
                names.add(stats[i].getPath().toString());
            }
        }

        fs.close();
        return names;
    }

    /**
     * 将HDFS上文件复制到HDFS上
     * @param src   原目标
     * @param dsc   复制到的目标
     * @throws Exception
     * @throws IllegalArgumentException
     */
    public void copy(String src,String dsc) throws Exception{
        /**
         * 1:建立输入流
         * 2：建立输出流
         * 3:两个流的对接
         * 4:资源的关闭
         */

        FileSystem fs=init();

        //1:建立输入流
        FSDataInputStream input=fs.open(new Path(src));

        //2:建立输出流
        FSDataOutputStream output=fs.create(new Path(dsc));

        //3:两个流的对接
        byte[] b= new byte[4096];
        int hasRead = 0;
        while((hasRead = input.read(b))>0){
            output.write(b, 0, hasRead);
        }

        //4:资源的关闭
        input.close();
        output.close();
        fs.close();

    }
    /**
     * 复制一个目录下面的所有文件
     * @param src   需要复制的文件夹或文件
     * @param dsc   目的地
     * @throws Exception
     * @throws FileNotFoundException
     */
    public void copyDir(String src,String dsc) throws Exception{
        FileSystem fs=init();
        Path srcPath=new Path(src);
        String[] strs=src.split("/");
        String lastName=strs[strs.length-1];
        if(fs.isDirectory(srcPath)){
            fs.mkdirs(new Path(dsc+"/"+lastName));

            //遍历
            FileStatus[] fileStatus=fs.listStatus(srcPath);
            for(FileStatus fileSta:fileStatus){
                copyDir(fileSta.getPath().toString(),dsc+"/"+lastName);
            }

        }else{
            fs.mkdirs(new Path(dsc));
            System.out.println("src"+src+"\n"+dsc+"/"+lastName);
            copy(src,dsc+"/"+lastName);
        }
    }

}
