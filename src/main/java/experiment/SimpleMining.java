package experiment;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import utils.StaticInfo;
/**
 * Apriori算法
 * 1.序列数据库完全保存在内存中
 * 2.每计算一次支持度都需要扫描一遍序列数据库
 * 3.Apriori性质剪枝
 * 4.directory：日志目录；gap：时间间隔限制；frequency：支持度阈值
 * */
public class SimpleMining {
	public static void main(String args[]) throws IOException, ParseException{
//		String directory = StaticInfo.EXPERIMENT_ONE_DIRECTORY;
		String directory = "/home/belan/Desktop/实验一数据";		
		SimpleMining sm = new SimpleMining(directory,(long)1000,3);			
		long time = System.currentTimeMillis();		
		Set<List<LogRecord>> result = sm.miningFrequentSequence();
		time = System.currentTimeMillis() - time;		
		for(List<LogRecord> seq:result){
			System.out.println(seq);
		}
		System.out.println("结果数量："+result.size());
		System.out.println("挖掘耗时:"+time);		
	}
	public static class LogRecord{
		String content;//日志内容
		long time;//时间戳
		public LogRecord(String content,long time){
			this.content = content;
			this.time = time;
		}
		@Override
		public boolean equals(Object obj){
			if(obj==this)return true;
			else if(obj instanceof LogRecord){
				LogRecord lr = (LogRecord)obj;
				if(lr.content.equals(this.content))
					return true;
			}
			return false;
		}
		@Override
		public int hashCode(){
			return content.hashCode();
		}
		@Override
		public String toString(){
			return content;
		}
	}
	/**
	 * 频繁序列模式挖掘<br/>
	 * directory：日志目录<br/>
	 * gap：时间间隔限制<br/>
	 * frequency：支持度阈值<br/>
	 * 构造方法负责加载日志序列数据，miningFrequentSequence()方法完成全部挖掘任务。
	 * */
	public SimpleMining(String directory,long gap,
			int frequence) throws IOException, ParseException{		
		this.gap = gap;
		this.frequence = frequence;
		this.sdf = new SimpleDateFormat(StaticInfo.DATE_STRING);
		//init words and logDataBase
		this.words = new ArrayList<LogRecord>(); 
		Set<LogRecord> wordSet = new HashSet<LogRecord>();
		this.logDataBase = new HashMap<String,List<LogRecord>>();
		File fileDir = new File(directory);
		File[] files = fileDir.listFiles();
		for(int i=0;i<files.length;i++){
			List<LogRecord> list = new ArrayList<LogRecord>();
			BufferedReader br = new BufferedReader(new FileReader(files[i]));
			String s = null;
			while((s=br.readLine())!=null){
				String[] ss = s.split(",");
				Date date = sdf.parse(ss[0]);
				String content = ss[1];
				LogRecord lr = new LogRecord(content,date.getTime());
				list.add(lr);
				wordSet.add(lr);
			}
			this.logDataBase.put(files[i].getName(), list);
			br.close();
		}		
		this.words.addAll(wordSet);
	}
	private long gap;
	private Map<String,List<LogRecord>> logDataBase;
	private List<LogRecord> words;
	private int frequence;	
	private SimpleDateFormat sdf;
	/**
	 * 挖掘频繁模式序列
	 * */
	public Set<List<LogRecord>> miningFrequentSequence(){
		Set<List<LogRecord>> fset = new HashSet<List<LogRecord>>();//结果集
		//计算长度为1的频繁序列frequentWords，构造第一层seqSet
		List<LogRecord> frequentWords = new ArrayList<LogRecord>();
		Set<List<LogRecord>> seqSet = new HashSet<List<LogRecord>>();
		for(int i=0;i<words.size();i++){
			List<LogRecord> seq = new ArrayList<LogRecord>();
			seq.add(words.get(i));
			if(frequency(seq)>=this.frequence){
				frequentWords.add(seq.get(0));
				seqSet.add(seq);
				fset.add(seq);
			}
		}
		//挖掘频繁序列模式
		while(true){			
			List<Set<List<LogRecord>>> result = enumerate(seqSet,frequentWords,fset);
			Set<List<LogRecord>> nextSeqSet = result.get(0);
			Set<List<LogRecord>> removeSeqSet = result.get(1);
			if(nextSeqSet.size()==0)break;
			fset.removeAll(removeSeqSet);//移除新频繁序列的-1子序列
			fset.addAll(nextSeqSet);//添加新的频繁序列
			seqSet = nextSeqSet;			
		}
		return fset;
	}
	//result[0]下一层频繁序列的集合-Set<List<LogRecord>>
	//result[1]fset中需要移除的序列的集合-Set<List<LogRecord>>
	public List<Set<List<LogRecord>>> enumerate(Set<List<LogRecord>> seqSet,
			List<LogRecord> frequentWords,Set<List<LogRecord>> fset){
		Set<List<LogRecord>> newSeqSet = new HashSet<List<LogRecord>>();
		Set<List<LogRecord>> removeSeqSet = new HashSet<List<LogRecord>>();
		for(List<LogRecord> seq:seqSet){
			for(LogRecord word:frequentWords){
				//create new seq
				List<LogRecord> nSeq = new ArrayList<LogRecord>();
				nSeq.addAll(seq);
				nSeq.add(word);
				if(newSeqSet.contains(nSeq))
					continue;
				//剪枝
				List<List<LogRecord>> sonSeqSet = prune(fset,nSeq);
				if(sonSeqSet.size()==2){
					if(frequency(nSeq)>=this.frequence){//加入到下一层中
						newSeqSet.add(nSeq);
						removeSeqSet.addAll(sonSeqSet);
					}
				}
			}
		}
		List<Set<List<LogRecord>>> result = new ArrayList<Set<List<LogRecord>>>();
		result.add(newSeqSet);
		result.add(removeSeqSet);
		return result;
	}
	//求seq去掉首尾的两个子序列；判断两个子序列是否在fset中，如果在则返回
	public List<List<LogRecord>> prune(Set<List<LogRecord>> fset,List<LogRecord> seq){
		List<List<LogRecord>> sonSeqSet = new ArrayList<List<LogRecord>>();//去掉首尾子序列集合
		//移除第一个元素
		List<LogRecord> sonSeqWithoutFirlstElement = new ArrayList<LogRecord>();
		for(int i=1;i<seq.size();i++)
			sonSeqWithoutFirlstElement.add(seq.get(i));
		if(fset.contains(sonSeqWithoutFirlstElement))
			sonSeqSet.add(sonSeqWithoutFirlstElement);			
		//移除最后一个元素
		sonSeqWithoutFirlstElement = new ArrayList<LogRecord>();
		for(int i=0;i<seq.size()-1;i++)
			sonSeqWithoutFirlstElement.add(seq.get(i));		
		if(fset.contains(sonSeqWithoutFirlstElement))
			sonSeqSet.add(sonSeqWithoutFirlstElement);		
		return sonSeqSet;
	}
	
	//返回seq在logDataBase中多少log sequence中出现过
	public int frequency(List<LogRecord> seq){
		int num = 0;
		for(Entry<String,List<LogRecord>> entry:
			logDataBase.entrySet()){//for each logFile
			List<LogRecord> log = entry.getValue();//log sequence
			for(int i=0;i<=log.size()-seq.size();i++){
				if(isMatch(log,i,-1,seq,0)){
					num++;
					break;
				}
			}
		}
		return num;
	} 
	public boolean isMatch(List<LogRecord> seq1,int curSeq1,
			long lastTime,List<LogRecord> seq2,int curSeq2){
		if(curSeq2>=seq2.size())return true;
		LogRecord cur = seq2.get(curSeq2);
		boolean flag = false;
		int i = curSeq1;
		for(;i<=seq1.size()-seq2.size()+curSeq2&&
				(lastTime<0||seq1.get(i).time-lastTime<=gap);i++){
			if(seq1.get(i).equals(cur)){
				flag = true;break;
			}
		}
		if(!flag)return false;
		return isMatch(seq1,i+1,seq1.get(i).time,seq2,curSeq2+1);
	}
}
