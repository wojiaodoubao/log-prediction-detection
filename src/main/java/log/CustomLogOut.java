package log;

import org.apache.commons.logging.Log;
/**
 * 常用日志输出
 * */
public class CustomLogOut {
	public static void resourceMutexLock(Log log,MutexResource mr){
		log.info("Resource Mutex Lock:"+mr.getResourceInfo());
	}
	public static void resourceMutexUnlock(Log log,MutexResource mr){
		log.info("Resource Mutex Unlock:"+mr.getResourceInfo());
	}
}