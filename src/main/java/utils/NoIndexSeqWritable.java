package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class NoIndexSeqWritable implements Writable,Comparable<NoIndexSeqWritable>{
	public List<SeqMeta> seq;
	public NoIndexSeqWritable(){
		//实现Writable接口类必须有一个无参构造方法
		//否则抛出java.lang.NoSuchMethodException: className.<init>()		
	}
	public NoIndexSeqWritable(List<SeqMeta> seq){
		this.seq = seq;
	}
	
	@Override
	public String toString(){
		StringBuffer sbuffer = new StringBuffer();
		if(seq!=null){
			for(SeqMeta m:seq){
				sbuffer.append(m+",");
			}
			if(sbuffer.charAt(sbuffer.length()-1)==','){
				sbuffer.deleteCharAt(sbuffer.length()-1);
			}
		}		
		return sbuffer.toString();
	}

	public static NoIndexSeqWritable createFromString(String s){
		NoIndexSeqWritable res = new NoIndexSeqWritable();
		createFromString(res,s);
		return res;
	}
	public static void createFromString(NoIndexSeqWritable nsw,String s){
		if(nsw.seq!=null)
			nsw.seq.clear();
		else
			nsw.seq = new ArrayList<SeqMeta>();
		//construct seq
		if(s!=null&&!s.equals("")){
			for(String tmp:s.split(",")){
				nsw.seq.add(SeqMeta.getSeqMetaBySID(Long.parseLong(tmp)));
			}
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
		NoIndexSeqWritable.createFromString(this,new String(bytes));		
	}
	
	private static final int reverse = -1;//反字典序
	public int compareTo(NoIndexSeqWritable o) {
		if(this==o)
			return 0;
		else if(o instanceof NoIndexSeqWritable){
			NoIndexSeqWritable nsw = (NoIndexSeqWritable)o;
			if(nsw.seq==null){
				if(this.seq==null)return 0;
				return 1*reverse;
			}
			else if(this.seq==null)return -1*reverse;
			int i=0,j=0;
			while(i<this.seq.size()&&j<nsw.seq.size()){
				if(this.seq.get(i).getSID()<nsw.seq.get(j).getSID())return -1*reverse;
				else if(this.seq.get(i).getSID()>nsw.seq.get(j).getSID())return 1*reverse;
				else{
					i++;j++;
				}
			}
			if(i<this.seq.size())return 1*reverse;
			else if(j<nsw.seq.size())return -1*reverse;
			return 0;
		}
		return -1*reverse;//返回值随意		
	}
	@Override
	public boolean equals(Object obj){
		if(this==obj)return true;
		else if(obj instanceof NoIndexSeqWritable){
			NoIndexSeqWritable nsw = (NoIndexSeqWritable)obj;
			return this.seq.equals(nsw.seq);
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
		//第一个NoIndexSeqWritable对象
		seq.add(SeqMeta.getSeqMetaBySID((long)0));
		seq.add(SeqMeta.getSeqMetaBySID((long)1));
		seq.add(SeqMeta.getSeqMetaBySID((long)2));
		//第二个NoneIndexSeqWritable对象
		NoIndexSeqWritable w = new NoIndexSeqWritable(seq);
		seq = new ArrayList<SeqMeta>();
		seq.add(SeqMeta.getSeqMetaBySID((long)5));
		NoIndexSeqWritable w2 = new NoIndexSeqWritable(seq);		
		//测试，写两遍读两遍，Pass
		NoIndexSeqWritable r = new NoIndexSeqWritable(null);
		RandomAccessFile out = new RandomAccessFile("/home/belan/Desktop/SequenceFile","rw");		
		w.write(out);
		w2.write(out);
		out.close();
		out = new RandomAccessFile("/home/belan/Desktop/SequenceFile","rw");
		r.readFields(out);
		System.out.println(r.seq);
		r.readFields(out);
		System.out.println(r.seq);	
		out.close();		
	}	
}
