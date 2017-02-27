package motif;
/**
 * 序列数据离散化后的单位元素
 * Raw data经过预处理之后得到的序列数据应该是一个Meta[]数组
 * */
public abstract class Meta {
	public abstract boolean equals(Meta m);
	public abstract String toString();
}
class LogMeta extends Meta{
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
	public boolean equals(Meta m) {
		if(this==m)return true;
		if(m instanceof LogMeta){
			LogMeta lm = (LogMeta)m;
			if(this.sid==lm.sid)return true;
		}
		return false;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "["+sid+"]";
	}	
}
class DimensionLogMeta extends LogMeta{
	protected int dimension;
	public DimensionLogMeta(long sid,int dimension){
		super(sid);
		this.dimension = dimension;
	}
	@Override
	public boolean equals(Meta m) {
		if(this==m)return true;
		if(m instanceof DimensionLogMeta){
			DimensionLogMeta dlm = (DimensionLogMeta)m;
			if(dlm.sid==this.sid&&dlm.dimension==this.dimension)return true;
		}
		return false;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "["+sid+"]";
	}	
}