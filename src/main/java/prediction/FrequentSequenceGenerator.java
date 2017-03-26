package prediction;

import java.util.*;
import java.util.Map.Entry;

import prediction.SeqWritable.Index;

public class FrequentSequenceGenerator {
	public static void main(String args[]){//测试
		/**
		 * 字符sid值：
		 * 5 4 3 2 1
		 * a b c d e
		 * 测试数据：
		 * Location:1 2 3 4 5 6 7
		 *       F1:a b c e a b a
		 *       F2:d e a b c
		 *       F3:e d e b a
		 * 结果：
		 * [2, 5]
		 * {F3=[(2 5)], F2=[(1 3)]}
		 * [2, 1, 5]
		 * {F3=[(2 5)], F2=[(1 3)]}
		 * [5, 3]
		 * {F1=[(1 3)], F2=[(3 5)]}
		 * [1, 5, 4]
		 * {F1=[(4 6)], F2=[(2 4)]}
		 * [1, 4, 5]
		 * {F1=[(4 7)], F3=[(1 5), (3 5)]}
		 * [5, 4, 3]
		 * {F1=[(1 3)], F2=[(3 5)]}
		 * */
		long p1 = 1;
		long p2 = 2;
		long p3 = 3;
		long p4 = 4;
		long p5 = 5;
		long p6 = 6;
		long p7 = 7;
		long gap = 10;
		int frequency = 2;
		List<SeqMeta> seq = null;//序列
		Map<String,List<Index>> indexMap = null;
		
		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(SeqMeta.getSeqMetaBySID((long)5));//a
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
		seq.add(SeqMeta.getSeqMetaBySID((long)4));//b
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
		seq.add(SeqMeta.getSeqMetaBySID((long)3));//c
		list = new ArrayList<Index>();
		list.add(new Index(p3,p3));
		indexMap.put("F1", list);
		list = new ArrayList<Index>();
		list.add(new Index(p5,p5));
		indexMap.put("F2", list);		
		SeqWritable C = new SeqWritable(seq,indexMap);

		seq = new ArrayList<SeqMeta>();
		indexMap = new HashMap<String,List<Index>>();
		seq.add(SeqMeta.getSeqMetaBySID((long)2));//d
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
		seq.add(SeqMeta.getSeqMetaBySID((long)1));//e
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
		seqSet = getFrequentSequence(seqSet,gap,frequency);
		for(SeqWritable sw:seqSet){
			System.out.println(sw.seq);
			System.out.println(sw.indexMap);
		}
	}
	/**
	 * Apriori算法(改)-发现频繁序列
	 * 
	 * 算法可以参照测试中的测试例子来看。
	 * 输入：
	 * seqSet:频繁1维序列，没有重复项
	 * gap:gap
	 * frequency:frequency
	 * 输出：
	 * 产生所有含max_sid且序列由<=sid元素组成的gap，frequency频繁序列。
	 * */
	public static Set<SeqWritable> getFrequentSequence(
			Set<SeqWritable> seqSet,long gap,int frequency){
		SeqWritable.reverse = -1;//递减序
		List<SeqWritable> one = new ArrayList<SeqWritable>();//size==1的频繁序列，每次递1的1都是从One中选的。
		one.addAll(seqSet);
		Collections.sort(one);
		
		Set<SeqWritable> result = new HashSet<SeqWritable>();//收集挖掘结果
		result.add(one.get(0));//收集size==1的含a的频繁序列
		int size = result.size();
		while(true){
			Set<SeqWritable> newSeqSet = enumerate(seqSet,one,gap,frequency);//挖掘下一层		
			//result中去掉子序列。ps:在挖掘n+1层的过程中不能去掉第n层中的子序列，比如aaaaa->aaaaaa，如果去掉aaaaa，那根据剪枝规则，以后的aaaaab就无法生成了。
			SeqWritable swTmp = new SeqWritable(null,null);
			for(SeqWritable sw:newSeqSet){
				swTmp.seq = dropAMeta(sw.seq,0);//去掉第一个 
				result.remove(swTmp);
				swTmp.seq = dropAMeta(sw.seq,sw.seq.size()-1);//去掉最后一个
				result.remove(swTmp);
			}
			//加入n+1层
			result.addAll(newSeqSet);
			//n=n+1
			seqSet = newSeqSet;
			//没有挖掘出新序列则退出
			if(result.size()<=size)break;
			else size = result.size();
		}		
		return result;
	}
	public static int frequency(SeqWritable sw){
		int i = 0;
		if(sw==null)return i;
		return sw.indexMap.size();//在多少文件中出现过
	}
	//只能每次长1，因为只知道所有size==1的频繁模式的完整Index
	private static Set<SeqWritable> enumerate(Set<SeqWritable> seqSet,
			List<SeqWritable> one,long gap,int frequency){
		Set<SeqWritable> newSeqSet = new HashSet<SeqWritable>();
		if(one==null||seqSet==null||one.size()<=0||seqSet.size()<=0)return newSeqSet;
		for(SeqWritable sw:seqSet){
			if(sw.seq.size()<=0)continue;
			else if(sw.seq.size()==1){//特殊处理size==1层，[a,b,c,d,e]->[aa,ab,ac,ad,ae,ba,ca,da,ea]
				SeqWritable tmp = merge(one.get(0),one.get(0),gap,frequency);
				if(tmp!=null)newSeqSet.add(tmp);					
				for(int i=1;i<one.size();i++){
					tmp = merge(one.get(0),one.get(i),gap,frequency);
					if(tmp!=null)newSeqSet.add(tmp);
					tmp = merge(one.get(i),one.get(0),gap,frequency);
					if(tmp!=null)newSeqSet.add(tmp);						
				}
			}
			else{//size>1层，剪枝合并
				SeqWritable swTmp = new SeqWritable(null,null);
				for(SeqWritable c:one){
					//剪枝，合并
					swTmp.seq = mergeSeq(sw,c);//sw+c
					if(!newSeqSet.contains(swTmp)){//已经在第n+1层？
						swTmp.seq = dropAMeta(swTmp.seq,0);
						if(!swTmp.seq.contains(one.get(0).seq.get(0))||seqSet.contains(swTmp)){
							//如果list是一个含a序列，则list需要在seqSet中出现
							SeqWritable tmp = merge(sw,c,gap,frequency);
							if(tmp!=null)newSeqSet.add(tmp);
						}
					}
					swTmp.seq = mergeSeq(c,sw);//c+sw
					if(!newSeqSet.contains(swTmp)){//已经在第n+1层？
						swTmp.seq = dropAMeta(swTmp.seq,swTmp.seq.size()-1);
						if(!swTmp.seq.contains(one.get(0).seq.get(0))||seqSet.contains(swTmp)){
							//如果list是一个含a序列，则list需要在seqSet中出现
							SeqWritable tmp = merge(c,sw,gap,frequency);
							if(tmp!=null)newSeqSet.add(tmp);
						}
					}					
				}				
			}
		}
		return newSeqSet;
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
	private static List<SeqMeta> dropAMeta(List<SeqMeta> list,int index){
		List<SeqMeta> result = new ArrayList<SeqMeta>();
		for(int i=0;i<list.size();i++){
			if(i==index)continue;
			result.add(list.get(i));
		}
		return result;
	}
	/** 
	 * 归并合并sw1，sw2；极大提高合并效率！
	 * 归并合并O(len(sw1)+len(sw2))>枚举合并O(len(sw1)*len(sw2))>Apriori扫描合并O(序列数据库条目数*在该条上做fired匹配)//序列数据库条目数==日志文件个数
	 * 1.在sw1后追加合并sw2，并检验频数是否>=frequency，满足返回合并后的新sw，否则返回null。
	 * 2.frequency可以是任意int值，可以是负值。
	 * 3.是根据sw1与sw2的index进行merge，调用者应该在保证搜集了完整的sw1，sw2索引信息。
	 * */
	public static SeqWritable merge(SeqWritable sw1,SeqWritable sw2,long gap,int frequency){
		Map<String,List<Index>> rindex = new HashMap<String,List<Index>>();
		for(Entry<String, List<Index>> entry:sw1.indexMap.entrySet()){//for each log in sw1
			List<Index> first = entry.getValue();
			List<Index> second = sw2.indexMap.get(entry.getKey());
			if(first==null||second==null||first.size()<=0||second.size()<=0)continue;
			//归并
			int i = 0;
			int j = 0;
			List<Index> list = new ArrayList<Index>();
			while(i<first.size()&&j<second.size()){
				if(first.get(i).end>=second.get(j).start)
					j++;
				else if(second.get(j).start-first.get(i).end>gap)
					i++;
				else{
					list.add(new Index(first.get(i).start,second.get(j).end));
					j++;
				}
			}
			if(list.size()>0)
				rindex.put(entry.getKey(), list);
		}
		if(rindex.size()>=frequency){
			List<SeqMeta> rlist = mergeSeq(sw1,sw2);
			return new SeqWritable(rlist,rindex);
		}
		return null;
	}
}
