package log;

import org.apache.commons.logging.Log;

/**
 * 目录互斥资源
 * 使用DirMutexResource的作业将互斥的获得目录锁
 * 示例代码:
 * DirMutexResource dmr = new DirMutexResource(info);
 * DirLock lock = null;
 * while((lock = dmr.lock())==null){
 * 	Thead.sleep(time);
 * }
 * //do something
 * dmr.unlock(lock);
 * */
class DirMutexResource extends MutexResource{
	private Log log = null;
	public DirMutexResource(String resourceInfo,Log log){
		super(resourceInfo);
		this.log = log;
	}
	public static class DirLock{
		String fileName;
	}
	@Override
	public DirLock lock() {
		/**
		 * if 已经存在.lock文件
		 * 	return null;
		 * else{
		 * 	添加jobname_timestamp.lock文件
		 * 	检查除了jobname_timestamp.lock外是否还有其他.lock文件：
		 * 		如果有:删除jobname_timestamp.lock；return null;
		 * 		否则：CustomLogOut.resourceMutexLock(log,this);return new DirLock(jobname_timestamp.lock);
		 * }
		 * */
		return new DirLock();
	}

	@Override
	public boolean unlock(Object lock) {
		if(!(lock instanceof DirLock))
			return false;
		/**
		 * 如果DirLock.fileName存在:删除DirLock.fileName
		 * */
		CustomLogOut.resourceMutexUnlock(log, this);
		return true;
	}
}
