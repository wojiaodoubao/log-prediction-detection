package experiment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.io.*;

import prediction.FrequentSequenceGenerator;
import utils.SeqMeta;
import utils.SeqWritable;
import utils.StaticInfo;
import utils.SeqWritable.Index;

public class MiningWithIndexAndSID {
	public static void main(String args[]) throws NumberFormatException, IOException{
//		String directory = StaticInfo.EXPERIMENT_ONE_DIRECTORY;
//		String directory = "/home/belan/Desktop/MetaFileSplit";
		String directory = "/home/belan/Desktop/正确输出数据2/MetaFileSplit";
		MiningWithIndexAndSID mi = new MiningWithIndexAndSID(directory,(long)6000,2);		
		long time = System.currentTimeMillis();
		Set<SeqWritable> res = mi.getFrequentSequence();
		time = System.currentTimeMillis()-time;
		for(SeqWritable sw:res)
			System.out.println(sw.seq+" "+sw.indexMap);
		System.out.println("结果数量："+res.size());
		System.out.println("挖掘耗时:"+time);		
	}
	public MiningWithIndexAndSID(String directory,long gap,
			int frequence) throws NumberFormatException, IOException{
		this.gap = gap;
		this.frequency = frequence;
		this.seqSet = createIndex(directory);
	}
	private Set<SeqWritable> seqSet;
	private long gap;
	private int frequency;
	/**
	 * 从预处理结果文件，反序列化SID-Index结构。
	 * */
	private Set<SeqWritable> createIndex(String directory) throws NumberFormatException, IOException{
		Set<SeqWritable> seqSet = new HashSet<SeqWritable>();
		SimpleDateFormat sdf = new SimpleDateFormat(StaticInfo.DATE_STRING);
		File fileDir = new File(directory);
		File[] files = fileDir.listFiles();
		for(int j=0;j<files.length;j++){
			if(files[j].getName().matches("_.*")||
					files[j].getName().matches("\\..*")){
				continue;
			}
			BufferedReader br = new BufferedReader(new FileReader(files[j]));
			String line = null;
			while((line=br.readLine())!=null){
				String[] s = line.toString().split("\t");
				if(s==null||s.length<=0)continue;
				//Construct meta list
				Long sid = Long.parseLong(s[0]);
				List<SeqMeta> list = new ArrayList<SeqMeta>();
				SeqMeta seqmeta = SeqMeta.getSeqMetaBySID(sid);
				list.add(seqmeta);
				//construct indexMap
				Map<String,List<Index>> indexMap = new HashMap<String,List<Index>>();
				for(int i=1;i<s.length-1;i+=2){//s[i]-logName s[i+1]-按','分隔的time
					List<Index> timelist = new ArrayList<Index>();
					for(String time:s[i+1].split(",")){
						Date date = null;
						try {
							date = sdf.parse(time);
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
				seqSet.add(sw);
			}
			br.close();
		}
		return seqSet;
	}
	/**
	 * 挖掘频繁序列模式
	 * */
	public Set<SeqWritable> getFrequentSequence(){
		List<SeqWritable> one = new ArrayList<SeqWritable>();//size==1的频繁序列，每次递1的1都是从One中选的。
		for(SeqWritable sw:seqSet){
			if(sw.indexMap.size()>=this.frequency)
				one.add(sw);
		}
		Set<SeqWritable> result = new HashSet<SeqWritable>();//收集挖掘结果
		result.addAll(one);//添加
		while(true){
			List<Set<SeqWritable>> nextLevel = enumerate(seqSet,one);//挖掘下一层 
			Set<SeqWritable> newSeqSet = nextLevel.get(0);
			Set<SeqWritable> removeSeqSet = nextLevel.get(1);
			SeqWritable swTmp = new SeqWritable(null,null);
			result.removeAll(removeSeqSet);
			result.addAll(newSeqSet);
			seqSet = newSeqSet;
			//没有挖掘出新序列则退出
			if(seqSet.size()<=0)break;
		}		
		return result;
	}
	//只能每次长1，因为只知道所有size==1的频繁模式的完整Index
	private List<Set<SeqWritable>> enumerate(Set<SeqWritable> seqSet,List<SeqWritable> one){
		Set<SeqWritable> newSeqSet = new HashSet<SeqWritable>();
		Set<SeqWritable> removeSeqSet = new HashSet<SeqWritable>();
		
		List<SeqMeta> linkedSeq = new LinkedList<SeqMeta>();
		SeqWritable swTmp = new SeqWritable(linkedSeq,null);
		for(SeqWritable sw:seqSet){
			linkedSeq.clear();
			linkedSeq.addAll(sw.seq);
			for(SeqWritable c:one){//剪枝，合并
				swTmp.seq.add(c.seq.get(0));//构造sw+c
				if(!newSeqSet.contains(swTmp)){//sw+c不在newSeqSet中
					SeqMeta element = swTmp.seq.remove(0);//构造sw[1:n-1]+c，即移除sw+c的第一个元素
					if(seqSet.contains(swTmp)){
						SeqWritable newSeq = FrequentSequenceGenerator.merge(sw,c,gap,frequency);//merge得到newSeq
						if(newSeq!=null){
							newSeqSet.add(newSeq);
							//添加到移除集合
							if(!removeSeqSet.contains(swTmp)){
								List<SeqMeta> removeSeq = new ArrayList<SeqMeta>();
								removeSeq.addAll(swTmp.seq);
								SeqWritable remove = new SeqWritable(removeSeq,null);
								removeSeqSet.add(remove);
							}
							removeSeqSet.add(sw);
						}
					}
					swTmp.seq.add(0, element);//还原为sw+c
				}
				swTmp.seq.remove(swTmp.seq.size()-1);//删除最后一个元素，还原为sw
			}				

		}
		List<Set<SeqWritable>> result = new ArrayList<Set<SeqWritable>>();
		result.add(newSeqSet);
		result.add(removeSeqSet);
		return result;
	}
	/**
	 * 合并SeqWritable，产生SeqMeta序列
	 * */
	private List<SeqMeta> mergeSeq(SeqWritable sw1,SeqWritable sw2){
		List<SeqMeta> list = new ArrayList<SeqMeta>();
		list.addAll(sw1.seq);
		list.addAll(sw2.seq);
		return list;
	}
	/**
	 * 返回丢弃list中索引为index的Meta<br/>
	 * 【警告】这样实现效率太低了，建议使用LinkedList<SeqMeta>，方便移除添加index meta。
	 * */
	private List<SeqMeta> dropAMeta(List<SeqMeta> list,int index){
		List<SeqMeta> result = new ArrayList<SeqMeta>();
		for(int i=0;i<list.size();i++){
			if(i==index)continue;
			result.add(list.get(i));
		}
		return result;
	}	
}
