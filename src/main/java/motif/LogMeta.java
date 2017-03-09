package motif;

public class LogMeta extends Meta{
	/**
	 * 暂时记录
	 * 
	 * 我在数据预处理的地方搞一个大Map，Map里存着哪条日志对应哪一个long值，然后构造序列的时候就
	 * meta[i] = new LogMeta(对应long值)就行了。
	 * */
	protected long sid;
	public LogMeta(long sid){
		this.sid = sid;
	}
	@Override
	public boolean equals(Object m) {
		if(this==m)return true;
		if(m instanceof LogMeta){
			LogMeta lm = (LogMeta)m;
			if(this.sid==lm.sid)return true;
		}
		return false;
	}
	@Override
	public String toString() {
		return "["+sid+"]";
	}	
}