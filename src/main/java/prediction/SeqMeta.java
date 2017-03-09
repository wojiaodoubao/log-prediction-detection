package prediction;

import java.util.HashMap;
import java.util.Map;

import motif.Meta;
/**
 * SeqMeta是不可变的
 * */
public class SeqMeta extends SIDMeta{
	private SeqMeta(long sid){
		super(sid);
	}
	@Override
	public int hashCode(){
		return (int)sid;
	}
	@Override
	public String toString() {
		return sid+"";
	}
	//为了节约空间，SeqMeta作为基本元是不可变的
	private static Map<Long,SeqMeta> metaMap = new HashMap<Long,SeqMeta>();
	/**
	 * 工程方法获取SeqMeta对象，SeqMeta对象是不可变的
	 * */
	public static SeqMeta getSeqMetaBySID(Long sid){
		if(metaMap.get(sid)==null)
			metaMap.put(sid, new SeqMeta(sid));
		return metaMap.get(sid);
	}
}
