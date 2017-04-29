package log;
/**
 * 互斥资源的基类
 * */
public abstract class MutexResource{
	private String resourceInfo;
	public MutexResource(String resourceInfo){
		this.resourceInfo = resourceInfo;
	}
	public String getResourceInfo(){
		return this.resourceInfo;
	}
	public abstract Object lock();
	public abstract boolean unlock(Object lock);
}