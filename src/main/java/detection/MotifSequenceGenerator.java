package detection;

import java.util.*;
import java.util.Map.Entry;

import motif.Meta;
import prediction.SeqMeta;
import prediction.SeqWritable;
import prediction.SeqWritable.Index;

public class MotifSequenceGenerator {
	public static void main(String args[]){//测试！
		//这里这里！！！！！测试起来！赶紧写完了可以干别的，加油加油！！！
		/**
		 * 字符sid值：
		 * 5 4 3 2 1
		 * a b c d e
		 * 测试数据：
		 * Location:1 2 3 4 5 6 7
		 *    +  F1:a b c e a b a
		 *    +  F2:d e a b c
		 *    -  F3:e d e b a
		 * 结果：
		 * 见：
		 * MotifSequenceGenerator测试结果
		 * MotifSequenceGenerator测试-按层的输出结果
		 * */
		long p1 = 1;
		long p2 = 2;
		long p3 = 3;
		long p4 = 4;
		long p5 = 5;
		long p6 = 6;
		long p7 = 7;
		List<SeqMeta> seq = null;//序列
		Map<String,List<Index>> indexMap = null;
		
		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(new SeqMeta(5));//a
		List<Index> list = new ArrayList<Index>();
		list.add(new Index(p1,p1));
		list.add(new Index(p5,p5));
		list.add(new Index(p7,p7));
		indexMap.put("F1", list);
		list = new ArrayList<Index>();
		list.add(new Index(p3,p3));
		indexMap.put("F2", list);
		list = new ArrayList<Index>();
		list.add(new Index(p5,p5));
		indexMap.put("F3", list);
		SeqWritable A = new SeqWritable(seq,indexMap);

		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(new SeqMeta(4));//b
		list = new ArrayList<Index>();
		list.add(new Index(p2,p2));
		list.add(new Index(p6,p6));
		indexMap.put("F1", list);
		list = new ArrayList<Index>();
		list.add(new Index(p4,p4));
		indexMap.put("F2", list);
		list = new ArrayList<Index>();
		list.add(new Index(p4,p4));
		indexMap.put("F3", list);
		SeqWritable B = new SeqWritable(seq,indexMap);

		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(new SeqMeta(3));//c
		list = new ArrayList<Index>();
		list.add(new Index(p3,p3));
		indexMap.put("F1", list);
		list = new ArrayList<Index>();
		list.add(new Index(p5,p5));
		indexMap.put("F2", list);		
		SeqWritable C = new SeqWritable(seq,indexMap);

		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(new SeqMeta(2));//d
		list = new ArrayList<Index>();
		list.add(new Index(p1,p1));
		indexMap.put("F2", list);
		list = new ArrayList<Index>();
		list.add(new Index(p2,p2));
		indexMap.put("F3", list);
		list = new ArrayList<Index>();
		SeqWritable D = new SeqWritable(seq,indexMap);

		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(new SeqMeta(1));//e
		list = new ArrayList<Index>();
		list.add(new Index(p4,p4));
		indexMap.put("F1", list);
		list = new ArrayList<Index>();
		list.add(new Index(p2,p2));
		indexMap.put("F2", list);
		list = new ArrayList<Index>();
		list.add(new Index(p1,p1));
		list.add(new Index(p3,p3));
		indexMap.put("F3", list);
		SeqWritable E = new SeqWritable(seq,indexMap);

		Set<SeqWritable> seqSet = new HashSet<SeqWritable>();
		seqSet.add(A);
		seqSet.add(B);
		seqSet.add(C);
		seqSet.add(D);
		seqSet.add(E);
		
		long gap = 10;
		double POD = 0.3;
		double FAR = 1;
		Map<String,Boolean> logLabel = new HashMap<String,Boolean>();
		logLabel.put("F1", true);
		logLabel.put("F2", true);
		logLabel.put("F3", false);
		seqSet = new MotifSequenceGenerator(seqSet,gap,POD,FAR,logLabel).getMotifSequence();
		for(SeqWritable sw:seqSet){
			System.out.println(sw.seq);
			System.out.println(sw.indexMap);
		}		
	}
	private List<SeqWritable> word;//word集合。word是一个序列，是MotifSequence的基本组成单位。
	private long gap;//meta间的gap限制；输入要自己保证word内gap限制。
	private double POD;//TP/(TP+FN)
	private double FAR;//FP/(TP+FP)
	private int Positive_Log_Num = 0;//正例数
	private int Negative_Log_Num = 0;//负例数
	private Map<String,Boolean> logLabel;//日志文件标记。logName-Label
	public MotifSequenceGenerator(Set<SeqWritable> wordSet,
			long gap,double POD,double FAR,Map<String,Boolean> logLabel){
		word = new ArrayList<SeqWritable>();
		word.addAll(wordSet);
		SeqWritable.reverse = -1;//递减序
		Collections.sort(word);
		this.gap = gap;
		this.POD = POD;
		this.FAR = FAR;
		this.logLabel = logLabel;
		//计算总正例数，负例数
		Positive_Log_Num = 0;
		Negative_Log_Num = 0;
		for(boolean label:this.logLabel.values()){
			if(label)Positive_Log_Num++;
			else Negative_Log_Num++;
		}
	}
	/**
	 * 获取满足POD+FAR的motif序列。<br \>
	 * 1.seq满足POD+FAR，seq的连续子序列seq'一定满足POD，但却不一定满足FAR。
	 * 2.算法会减去连续子序列，即如果seq与seq'同时满足POD+FAR，则只会保留seq。
	 * 3.删去连续子序列，是考虑到有长的就尽量不用短的，可以减少输出。
	 * */
	public Set<SeqWritable> getMotifSequence(){
		Set<SeqWritable> resultMotif = new HashSet<SeqWritable>();//结果集
		int size = resultMotif.size();//当前层号
		Set<SeqWritable> curMotifs = new HashSet<SeqWritable>(word);//构造当前层motifs
		while(true){			
			//输出
//			for(SeqWritable sw:curMotifs){
//				System.out.println(sw.seq);
//				System.out.println(sw.indexMap);
//			}	
//			System.out.println("--------------------");		
			Set<SeqWritable> nextMotifs = enumerate(curMotifs);//挖掘下一层		
			//resultMotif中去掉子序列。
			//seq满足POD+FAR，seq的连续子序列seq'一定满足POD却不一定满足FAR
			//这里仍然要删去子序列，是考虑到有长的就尽量不用短的，否则长的出现的时候还要把短的也记一次。
			SeqWritable swTmp = new SeqWritable(null,null);
			for(SeqWritable sw:nextMotifs){
				swTmp.seq = dropAMeta(sw.seq,0);//去掉第一个 
				resultMotif.remove(swTmp);
				swTmp.seq = dropAMeta(sw.seq,sw.seq.size()-1);//去掉最后一个
				resultMotif.remove(swTmp);
				double[] score = compute_POD_FAR_CSI(sw);
				if(score!=null&&score.length>=3&&score[1]<=this.FAR)//如果FAR合法，加入结果集
					resultMotif.add(sw);
			}
			//n=n+1
			curMotifs = nextMotifs;
			//没有挖掘出新序列则退出
			if(resultMotif.size()<=size)break;
			else size = resultMotif.size();
		}		
		return resultMotif;		
	}
	/**
	 * 返回下一层候选motifSet
	 * currentMotif:当前层motif集合。
	 * 【注】返回的motif是POD合法motif，不一定是POD+FAR合法motif。因为所有POD合法motif的后续motif都可能是POD+FAR合法motif。
	 * 因此调用者应该自行过滤，将POD+FAR合法的motif加入结果集，同时保留所有POD合法motif作为下一次迭代的输入。 
	 * */
	private Set<SeqWritable> enumerate(Set<SeqWritable> currentMotif){
		Set<SeqWritable> nextMotif = new HashSet<SeqWritable>();
		if(word==null||currentMotif==null||word.size()<=0||currentMotif.size()<=0)return nextMotif;
		for(SeqWritable sw:currentMotif){
			if(sw.seq.size()<=0)continue;
			else if(sw.seq.size()==1){//size==1层，特殊处理，[a,b,c,d,e]->[aa,ab,ac,ad,ae,ba,ca,da,ea]
				SeqWritable tmp = null;
				for(int i=0;i<word.size();i++){
					tmp = merge(word.get(0),word.get(i));
					if(tmp!=null)nextMotif.add(tmp);
					if(i==0)continue;
					tmp = merge(word.get(i),word.get(0));
					if(tmp!=null)nextMotif.add(tmp);					
				}
			}
			else{//size>1层，剪枝合并
				SeqWritable swTmp = new SeqWritable(null,null);
				for(SeqWritable c:word){
					//剪枝，合并
					swTmp.seq = mergeSeq(sw,c);//sw+c
					if(!nextMotif.contains(swTmp)){//已经在第n+1层？
						swTmp.seq = dropAMeta(swTmp.seq,0);
						if(!swTmp.seq.contains(word.get(0).seq.get(0))||currentMotif.contains(swTmp)){
							//如果swTmp.seq是一个含a序列，则swTmp.seq需要currentMotif在中出现
							SeqWritable tmp = merge(sw,c);
							if(tmp!=null)nextMotif.add(tmp);
						}
					}
					swTmp.seq = mergeSeq(c,sw);//c+sw
					if(!nextMotif.contains(swTmp)){//已经在第n+1层？
						swTmp.seq = dropAMeta(swTmp.seq,swTmp.seq.size()-1);
						if(!swTmp.seq.contains(word.get(0).seq.get(0))||currentMotif.contains(swTmp)){
							//如果swTmp.seq是一个含a序列，则swTmp.seq需要currentMotif在中出现
							SeqWritable tmp = merge(c,sw);
							if(tmp!=null)nextMotif.add(tmp);
						}
					}					
				}				
			}
		}
		return nextMotif;		
	}
	/** 
	 * 1.在sw1后追加合并sw2，并检验POD值是否>=this.POD，满足则返回合并后的新sw，否则返回null。<br/>
	 * 2.根据sw1与sw2的index信息进行merge和POD计算。
	 * */
	public SeqWritable merge(SeqWritable sw1,SeqWritable sw2){
		Map<String,List<Index>> rindex = new HashMap<String,List<Index>>();//序列'sw1sw2'的索引信息
		for(Entry<String, List<Index>> entry:sw1.indexMap.entrySet()){//for each logName in sw1
			List<Index> first = entry.getValue();
			List<Index> second = sw2.indexMap.get(entry.getKey());
			if(first==null||second==null||first.size()<=0||second.size()<=0)continue;
			for(Index fi:first){//O(n^2)，匹配sw1后接sw2的所有可能
				for(Index si:second){
					if(si.start-fi.end>0&&si.start-fi.end<=gap){
						List<Index> list = rindex.get(entry.getKey());
						if(list==null){
							list = new ArrayList<Index>();
							rindex.put(entry.getKey(), list);
						}
						list.add(new Index(fi.start,si.end));
					}
				}
			}
		} 
		List<SeqMeta> rlist = mergeSeq(sw1,sw2);
		SeqWritable sw = new SeqWritable(rlist,rindex);
		double[] pfc = compute_POD_FAR_CSI(sw);
		if(pfc!=null&&pfc.length>=3&&pfc[0]>=this.POD)
			return sw;
		return null;
	}		
	/**
	 * 计算序列的POD，FAR，CSI值
	 * */
	private double[] compute_POD_FAR_CSI(SeqWritable sw){
		return compute_POD_FAR_CSI(sw,this.logLabel,this.Positive_Log_Num,
				this.Negative_Log_Num);
	}
	/**
	 * 合并SeqWritable，产生SeqMeta序列
	 * */
	private static List<SeqMeta> mergeSeq(SeqWritable sw1,SeqWritable sw2){
		List<SeqMeta> list = new ArrayList<SeqMeta>();
		list.addAll(sw1.seq);
		list.addAll(sw2.seq);
		return list;
	}
	/**
	 * 返回list去掉第index个Meta后的list
	 * */
	private static List<SeqMeta> dropAMeta(List<SeqMeta> list,int index){
		List<SeqMeta> result = new ArrayList<SeqMeta>();
		for(int i=0;i<list.size();i++){
			if(i==index)continue;
			result.add(list.get(i));
		}
		return result;
	}	
	/**
	 * sw是否满足POD限制<br/>
	 * sw:motif序列
	 * POD:POD下界
	 * logLabel:日志文件标记
	 * */
	public static boolean satisfiedPOD(SeqWritable sw,double POD,Map<String,Boolean> logLabel){
		//计算总正例数，负例数
		int Positive_Log_Num = 0;
		int Negative_Log_Num = 0;
		for(boolean label:logLabel.values()){
			if(label)Positive_Log_Num++;
			else Negative_Log_Num++;
		}
		return satisfiedPOD(sw,POD,logLabel,Positive_Log_Num,Negative_Log_Num);
	}
	public static boolean satisfiedPOD(SeqWritable sw,double POD,Map<String,Boolean> logLabel,
			int Positive_Log_Num,int Negative_Log_Num){
		double[] score = compute_POD_FAR_CSI(sw, logLabel, Positive_Log_Num, Negative_Log_Num);
		System.out.println(score[0]);
		if(score!=null&&score.length>=3&&score[0]>=POD)
			return true;
		return false;
	}
	/**
	 * sw是否满足FAR限制<br/>
	 * sw:motif序列
	 * FAR:FAR上界
	 * logLabel:日志文件标记
	 * */
	public static boolean satisfiedFAR(SeqWritable sw,double FAR,Map<String,Boolean> logLabel){
		//计算总正例数，负例数
		int Positive_Log_Num = 0;
		int Negative_Log_Num = 0;
		for(boolean label:logLabel.values()){
			if(label)Positive_Log_Num++;
			else Negative_Log_Num++;
		}
		return satisfiedFAR(sw,FAR,logLabel,Positive_Log_Num,Negative_Log_Num);
	}
	public static boolean satisfiedFAR(SeqWritable sw,double FAR,Map<String,Boolean> logLabel,
			int Positive_Log_Num,int Negative_Log_Num){
		double[] score = compute_POD_FAR_CSI(sw, logLabel, Positive_Log_Num, Negative_Log_Num);
		if(score!=null&&score.length>=3&&score[1]<=FAR)
			return true;
		return false;
	}
	/**
	 * sw是否满足POD,FAR限制<br/>
	 * sw:motif序列
	 * POD:POD下界
	 * FAR:FAR上界
	 * logLabel:日志文件标记
	 * */
	public static boolean satisfiedPOD_FAR(SeqWritable sw,double POD,double FAR,Map<String,Boolean> logLabel){
		//计算总正例数，负例数
		int Positive_Log_Num = 0;
		int Negative_Log_Num = 0;
		for(boolean label:logLabel.values()){
			if(label)Positive_Log_Num++;
			else Negative_Log_Num++;
		}
		return satisfiedPOD_FAR(sw,POD,FAR,logLabel,Positive_Log_Num,Negative_Log_Num);
	}
	public static boolean satisfiedPOD_FAR(SeqWritable sw,double POD,double FAR,Map<String,Boolean> logLabel,
			int Positive_Log_Num,int Negative_Log_Num){
		double[] score = compute_POD_FAR_CSI(sw, logLabel, Positive_Log_Num, Negative_Log_Num);
		if(score!=null&&score.length>=3&&score[1]<=FAR&&score[0]>=POD)
			return true;
		return false;
	}	
	/**
	 * 计算POD，FAR，CSI值
	 * */
	public static double[] compute_POD_FAR_CSI(SeqWritable sw,Map<String,Boolean> logLabel,
			int Positive_Log_Num,int Negative_Log_Num){
		double[] result = new double[3];
		if(sw==null)
			return null;
		if(sw.indexMap==null||sw.indexMap.size()==0){//说明此序列是不存在的
			result[0] = 0;
			result[1] = 1;
			result[2] = 0;
			return result;
		}
		int TP = 0;
		int FP = 0;
		int FN = 0;
		int TN = 0;
		for(Entry<String, List<Index>> entry:sw.indexMap.entrySet()){//以日志文件为单位枚举索引
			Boolean label = logLabel.get(entry.getKey());//日志文件label
			if(label==null)//出现位置对应的文件没有label
				continue;
			else if(label)
				TP += entry.getValue().size();
			else
				FP += entry.getValue().size();
		}
		double POD = Positive_Log_Num==0?0:TP/(double)Positive_Log_Num;
		double FAR = (TP+FP)==0?0:FP/(double)(TP+FP);
		double CSI = TP/(double)(Positive_Log_Num+FP);
		result[0] = POD;
		result[1] = FAR;
		result[2] = CSI;
		return result;		
	}
}
