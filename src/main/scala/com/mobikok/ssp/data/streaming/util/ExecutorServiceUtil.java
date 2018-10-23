package com.mobikok.ssp.data.streaming.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServiceUtil {

	public static ExecutorService createdExecutorService(int threadPoolSize){
		//获取当前系统的CPU 数目
		int cpuNums = Runtime.getRuntime().availableProcessors();
	    //ExecutorService通常根据系统资源情况灵活定义线程池大小
		ExecutorService executorService = Executors.newFixedThreadPool(/*cpuNums **/ threadPoolSize);
		
		return executorService;
	}


}
