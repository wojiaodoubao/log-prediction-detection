package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import preprocessing.DataPreprocessing;
import utils.SeqWritable.Index;

public class SeqWritable implements Writable,Comparable<SeqWritable>{
	public List<SeqMeta> seq;//序列
	public Map<String,List<Index>> indexMap;//索引 <文件名，毫秒list>
	public static class Index{
		public long start;
		public long end;
		public Index(long start,long end){
			this.start = start;
			this.end = end;
		}
		@Override
		public String toString(){
			return "("+start+" "+end+")";
		}
	}
	public SeqWritable(){
		//实现Writable接口类必须有一个无参构造方法
		//否则抛出java.lang.NoSuchMethodException: className.<init>()
	}
	public SeqWritable(List<SeqMeta> seq,Map<String,List<Index>> indexMap){
		this.seq = seq;
		this.indexMap = indexMap;
	}
	
	public static final String SID_LOG_SPLIT = "\t";
	public static final String TIME_SPLIT = ",";
	
	@Override
	public String toString(){
		StringBuffer s = new StringBuffer();
		//construct seq
		if(seq!=null&&seq.size()>0){
			for(SeqMeta m:seq){
				s.append(m+SeqWritable.TIME_SPLIT);
			}
			if(s.charAt(s.length()-1)==','){
				s.deleteCharAt(s.length()-1);
			}
		}
		s.append(SID_LOG_SPLIT);
		//construct index
		if(indexMap!=null&&indexMap.size()>0){
			for(Entry<String, List<Index>> entry:indexMap.entrySet()){
				s.append(entry.getKey()+SeqWritable.TIME_SPLIT);
				if(entry.getValue()!=null){
					for(Index l:entry.getValue()){
						s.append(l.start+SeqWritable.TIME_SPLIT+l.end+SeqWritable.TIME_SPLIT);
					}
				}
				if(s.charAt(s.length()-1)==',')s.deleteCharAt(s.length()-1);
				s.append(SID_LOG_SPLIT);
			}
			if(s.charAt(s.length()-1)=='\t')s.deleteCharAt(s.length()-1);			
		}
		return s.toString();
	}
	/**
	 * 根据SeqWritable.toString()的字符串，构造SeqWritable对象，并返回
	 * */	
	public static SeqWritable createSeqWritableFromString(String s){
		SeqWritable res = new SeqWritable();
		try {
			createSeqWritableFromString(res,s);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	/**
	 * 根据SeqWritable.toString()的字符串，构造SeqWritable对象，存入sw<br />
	 * sw不能为null
	 * @throws Exception 
	 * */
	public static void createSeqWritableFromString(SeqWritable sw,String s) throws Exception{
		if(sw==null)
			throw new Exception("The input SeqWritable obj shouldn't be null! ");
		//init sw
		if(sw.seq!=null)
			sw.seq.clear();
		else
			sw.seq = new ArrayList<SeqMeta>();
		if(sw.indexMap!=null)
			sw.indexMap.clear();
		else
			sw.indexMap = new HashMap<String,List<Index>>();
		//split s
		String[] split = s.split(SeqWritable.SID_LOG_SPLIT);
		//construct seq
		if(split.length>0&&!split[0].equals("")){
			for(String sid:split[0].split(SeqWritable.TIME_SPLIT)){
				sw.seq.add(SeqMeta.getSeqMetaBySID(Long.parseLong(sid)));
			}
		}
		//construct index 
		for(int i=1;i<split.length;i++){
			List<Index> list = new ArrayList<Index>();
			String logName = null;
			String[] ttt = split[i].split(SeqWritable.TIME_SPLIT);
			for(int j=0;j<ttt.length-1;j++){
				if(j==0)logName = ttt[j];
				else{
					long start = Long.parseLong(ttt[j]);
					long end = Long.parseLong(ttt[j+1]);
					list.add(new Index(start,end));
					j++;
				}
			}
			sw.indexMap.put(logName, list);
		}
	}	
	public void write(DataOutput out) throws IOException {
		String s = this.toString();
		int size = s.getBytes().length;
		out.writeInt(size);//先写长度
		out.write(s.getBytes());//再写bytes
	}
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();//先获取长度
		byte[] bytes = new byte[size];
		in.readFully(bytes);//再读bytes，长度为size
		try {
			SeqWritable.createSeqWritableFromString(this,new String(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}
	@Override
	public SeqWritable clone(){
		List<SeqMeta> list = new ArrayList<SeqMeta>();//序列
		Map<String,List<Index>> map = new HashMap<String,List<Index>>();//索引 <文件名，毫秒list>
		for(SeqMeta m:seq)
			list.add(SeqMeta.getSeqMetaBySID(m.getSID()));
		for(Entry<String,List<Index>> entry:indexMap.entrySet()){
			List<Index> l = new ArrayList<Index>();
			for(Index index:entry.getValue())
				l.add(new Index(index.start,index.end));
			map.put(entry.getKey(), l);//String不用深拷贝。String的设计使其和基本类型一样，而不是像引用类型。
		}
		return new SeqWritable(list,map);
	}
	private static final String DATE_STRING = "yyyy-MM-dd hh:mm:ss";
	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_STRING);
	/**
	 * 根据字符反序列化SeqWritable对象。字符串采用MetaFileSplit的输出格式。<br/>
	 * 字符串格式：
	 * sid+'\t'+logName1+'\t'+time1,time2,time3,...,timen+'\t'+logName2+'\t'+time1,time2...
	 * 例：
	 * 3	log1	2016-06-16 10:45:53,2016-06-16 10:45:57	log2	2016-06-16 10:45:55	log3	2016-06-16 10:45:55
	 * 4	log1	2016-06-16 10:45:52,2016-06-16 10:45:56,2016-06-16 10:45:58	log2	2016-06-16 10:45:54	log3	2016-06-16 10:45:56 
	 * */
	public static SeqWritable deserializeSeqWritableFromString(String text){
		String[] s = text.split(SID_LOG_SPLIT);
		if(s==null||s.length<=0)return null;
		//Construct meta list
		long sid = Long.parseLong(s[0]);
		List<SeqMeta> list = new ArrayList<SeqMeta>();
		list.add(SeqMeta.getSeqMetaBySID(sid));
		//construct indexMap
		Map<String,List<Index>> indexMap = new HashMap<String,List<Index>>();
		for(int i=1;i<s.length-1;i+=2){//s[i]-logName s[i+1]-按','分隔的time
			List<Index> timelist = new ArrayList<Index>();
			for(String time:s[i+1].split(SeqWritable.TIME_SPLIT)){
				Date date = null;
				try {
					date = simpleDateFormat.parse(time);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if(date!=null){
					timelist.add(new Index(date.getTime(),date.getTime()));
				}
			}
			indexMap.put(s[i], timelist);
		}
		SeqWritable sw = new SeqWritable(list,indexMap);
		return sw;
	}

	private static final int reverse = -1;//reverse = 1;
	
	public int compareTo(SeqWritable obj) {
		if(this==obj)return 0;
		else if(obj instanceof SeqWritable){
			SeqWritable sw = (SeqWritable)obj;
			if(sw.seq==null){
				if(this.seq==null)return 0;
				return 1*reverse;
			}
			else if(this.seq==null)return -1*reverse;
			int i=0,j=0;
			while(i<this.seq.size()&&j<sw.seq.size()){
				if(this.seq.get(i).getSID()<sw.seq.get(j).getSID())return -1*reverse;
				else if(this.seq.get(i).getSID()>sw.seq.get(j).getSID())return 1*reverse;
				else{
					i++;j++;
				}
			}
			if(i<this.seq.size())return 1*reverse;
			else if(j<sw.seq.size())return -1*reverse;
			return 0;//这里修改！
		}
		return -1*reverse;//obj不是SeqWritable类型，随便返回
	}
	@Override
	public boolean equals(Object obj){
		if(this==obj)return true;
		else if(obj instanceof SeqWritable){
			SeqWritable sw = (SeqWritable)obj;
			return this.seq.equals(sw.seq);
		}
		return false;
	}
	@Override
	public int hashCode(){
		int hash = 0;
		if(seq!=null){
			for(SeqMeta m:seq){
				hash+=m.hashCode();
			}
		}
		return hash;
	}

	public static void main(String args[]) throws IOException{
		List<SeqMeta> seq = new ArrayList<SeqMeta>();//序列
		Map<String,List<Index>> indexMap = new HashMap<String,List<Index>>();
		seq.add(SeqMeta.getSeqMetaBySID((long)0));
		seq.add(SeqMeta.getSeqMetaBySID((long)1));
		seq.add(SeqMeta.getSeqMetaBySID((long)2));
		List<Index> list = new ArrayList<Index>();
		list.add(new Index((long) 10,(long)10));
		list.add(new Index((long) 11,(long)11));
		list.add(new Index((long) 12,(long)12));
		indexMap.put("XX", list);
		list = new ArrayList<Index>();
		list.add(new Index((long) 110,(long)110));
		list.add(new Index((long) 120,(long)120));
		list.add(new Index((long) 130,(long)130));
		indexMap.put("XXX", list);	
		//测试，写两遍读两遍，确实对
		SeqWritable w = new SeqWritable(seq, indexMap);
//		SeqWritable w = new SeqWritable(seq, null);
		seq = new ArrayList<SeqMeta>();
		seq.add(SeqMeta.getSeqMetaBySID((long)5));
		//seq==null，indexMap==null，4种组合，均可完成wirte&readFields
//		SeqWritable w2 = new SeqWritable(seq,indexMap);
		SeqWritable w2 = new SeqWritable(null, null);
//		SeqWritable w2 = new SeqWritable(null, indexMap);
//		SeqWritable w2 = new SeqWritable(seq, null);
		
		System.out.println("#"+w2+"#");
		
		SeqWritable r = new SeqWritable(null,null);
		RandomAccessFile out = new RandomAccessFile("/home/belan/Desktop/SequenceFile","rw");		
		w.write(out);
		w2.write(out);
		out.close();
		out = new RandomAccessFile("/home/belan/Desktop/SequenceFile","rw");
		r.readFields(out);
		System.out.println("%"+r+"%");
		r.readFields(out);
		System.out.println("%"+r+"%");
		out.close();		
	}
}
