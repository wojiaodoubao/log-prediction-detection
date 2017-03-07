package prediction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import motif.Meta;

public class RuleDiscovery {
	public static void main(String args[]){//测试
		//abceaba|deabc|edeba
		//5431545|21543|12145
		//得分： 0        1  2  3
		//  -2147483648  18 16 52
		//因为我测试数据中两meta间间距都是1，而gap=5；可以自己找一个所有的:a(5/3),ab(5/3),abc(2/2)，显然abc推算出的是最准的。
		TimeMeta[] logSeq = new TimeMeta[17];
		logSeq[0] = new TimeMeta(5,1);
		logSeq[1] = new TimeMeta(4,2);
		logSeq[2] = new TimeMeta(3,3);
		logSeq[3] = new TimeMeta(1,4);
		logSeq[4] = new TimeMeta(5,5);
		logSeq[5] = new TimeMeta(4,6);
		logSeq[6] = new TimeMeta(5,7);
		logSeq[7] = new TimeMeta(2,8);
		logSeq[8] = new TimeMeta(1,9);
		logSeq[9] = new TimeMeta(5,10);
		logSeq[10] = new TimeMeta(4,11);
		logSeq[11] = new TimeMeta(3,12);
		logSeq[12] = new TimeMeta(1,13);
		logSeq[13] = new TimeMeta(2,14);
		logSeq[14] = new TimeMeta(1,15);
		logSeq[15] = new TimeMeta(4,16);
		logSeq[16] = new TimeMeta(5,17);
		Meta[] freSeq = new TimeMeta[4];
		freSeq[0] = new TimeMeta(5,1);
		freSeq[1] = new TimeMeta(4,1);
		freSeq[2] = new TimeMeta(3,1);
		freSeq[3] = new TimeMeta(1,1);
		int[] res = new RuleDiscovery(logSeq,4,5,8,0.5).scoreAllRules(freSeq);
		for(int i:res)
			System.out.println(i);
	}
	private TimeMeta[] logSeq;//日志序列。
	private int cardinality;//Meta的基数。比如cardinality=8则Meta取值空间为8。
	private int bit;//由基数计算，表示一个Meta需要几位bit表示。
	private long gap;//两个Meta间的间隔gap			[meta1]gap[meta2]gap[meta3] --ruleGap--> [meta4]gap[meta5]
	private long ruleGap;//规则A->B中，A和B间最大间隔
	private double firedThreshold;//规则A->B，logSeq中前缀与A不同的位数/|A|<=firedThreshold。
	private static class Index implements Comparable{
		int start;
		int end;
		int distance;
		public Index(int start,int end,int distance){
			this.start = start;
			this.end = end;
			this.distance = distance;
		}
		public int compareTo(Object o) {
			if(this == o)
				return 0;
			else if(o!=null && o instanceof Index){
				Index index = (Index)o;
				if(distance<index.distance)
					return -1;
				else if(distance>index.distance)
					return 1;
				else
					return 0;
			}
			else
				return 1;
		}
		@Override
		public String toString(){
			return "("+start+" "+end+" "+distance+")";
		}
	}
	public RuleDiscovery(TimeMeta[] logSeq,int cardinality,long gap,long ruleGap,double firedThreshold){
		this.logSeq = logSeq;
		this.cardinality = cardinality;
		this.gap = gap;
		this.ruleGap = ruleGap;
		this.firedThreshold = firedThreshold;
		if(cardinality>0){
			//计算表示一个Meta需要的bit数
			double r = (Math.log(cardinality)/Math.log(2.0));//log2(cardinality)，表示这些Meta需要多少位！
			if(r-(int)r==0)bit = (int)r;
			else bit = 1+(int)r;
		}
	}
	/**
	 * 计算freSeq所有划分点下的规则分数
	 * 
	 * Input:
	 * freSeq:Meta[]，待划分频繁序列 
	 * Output:
	 * 不同划分下的打分，第i个位置表示freSeq[0~i-1]->freSeq[i,length-1]的分数。
	 * */
	public int[] scoreAllRules(Meta[] freSeq){
		int[] result = new int[freSeq.length];
		result[0] = Integer.MIN_VALUE;
		List<Index> subSeq = null;
		//枚举划分点  对于logSeq中所有长为sp的子序列，按照与R[0~sp-1]的距离由小到大，返回在logSeq中的索引Index(start,end)
		for(int sp=1;sp<freSeq.length;sp++){//freSeq[0,sp-1]->freSeq[sp,R.length-1]
			if(sp==1){
				subSeq = sizeOneSubsequences(freSeq[sp-1]);
			}
			else{
				subSeq = iterateIndex(subSeq,freSeq[sp-1]);
			}
//			System.out.println(subSeq);
			List<Index> firedSeq = filtrateAndSort(subSeq,sp);
//			System.out.println(firedSeq);
			//compute total bit save for result[sp];
			int total = 0;
			int n = 0;
			//对每个firedSeq计算bit-save
			for(n=0;n<firedSeq.size();n++){
				int subConsequentBits = Huffman(freSeq.length-sp);
				int smallestDistance = freSeq.length-sp;
				//firedSeq[n]->X   --X是一个序列；X起始time-firedSeq[n].time>ruleGap；X内任意相邻meta间隔<gap；
				//logSeq[i].time-logSeq[firedSeq.get(n).end].time>ruleGap
				//枚举X每个可能的startPoint。
				for(int i=firedSeq.get(n).end+1;i<=logSeq.length-(freSeq.length-sp);i++){
					if(logSeq[i].time-logSeq[firedSeq.get(n).end].time>ruleGap)break;
					int tmp = smallestDistance(i,freSeq,sp);
					smallestDistance = tmp<smallestDistance?tmp:smallestDistance;
				}
				int subConsequentMDLbits = MDL(smallestDistance);
				if(subConsequentBits-subConsequentMDLbits<=0)break;
				total += subConsequentBits-subConsequentMDLbits;
			}
			total -= Huffman(freSeq.length-1-sp+1);
			result[sp] = total;
		}	
		return result;
	}
	/**
	 * 返回logSeq所有size==1的子序列Index<br/>
	 * Index(start,end,与m的距离)
	 * */
	private List<Index> sizeOneSubsequences(Meta m){
		List<Index> list = new ArrayList<Index>();
		for(int i=0;i<logSeq.length;i++){
			if(logSeq[i].equals(m))
				list.add(new Index(i,i,0));
			else
				list.add(new Index(i,i,1));
		}
		return list;
	}
	/**
	 * 返回logSeq所有长度为n+1的子序列<br/>
	 * list:logSeq所有长度为n的子序列<br/>
	 * m:freSeq第n+1个元素<br/>
	 * gap:频繁序列gap限制
	 */
	private List<Index> iterateIndex(List<Index> list,Meta m){
		List<Index> newList = new ArrayList<Index>();
		for(int i=0;i<list.size();i++){
			Index in = list.get(i);
			for(int j=in.end+1;j<logSeq.length;j++){
				if((logSeq[j].time-logSeq[in.end].time)>gap)break;
				if(logSeq[j].equals(m))
					newList.add(new Index(in.start,j,in.distance));
				else
					newList.add(new Index(in.start,j,in.distance+1));
			}
		}
		return newList;
	}
	/**
	 * 按照递增序，返回list中所有满足firedThreshold的子序列<br/>
	 * length：list中序列的长度(因为Index只保存了首末位置和距离，没有长度信息)。
	 * */
	private List<Index> filtrateAndSort(List<Index> list,int length){
		List<Index> res = new ArrayList<Index>();
		for(Index in:list){
			if(firedThreshold>=in.distance/length)
				res.add(in);
		}
		Collections.sort(res);
		return res;
	}
	/**
	 * 此处并不是哈夫曼编码，而是等长编码。<br \>
	 * 论文中在形式化定义bit_save时使用的等长编码。这里尊重文章，使用等长编码。<br \>
	 * 论文后来在算法描述表中对函数命名使用了Huffman，我认为是笔误，这里尊重文章，对函数命名为Huffman。<br \>
	 * return length*bit;
	 * */
	private int Huffman(int length){//就是DL()函数，编码T[x-y]需要的二进制码位数
		return length*bit;//cardinality设置为2^64。
	}
	/**
	 * MDL计算两个序列间不同之处带来的bit消耗。两个序列分别是firedSeq[i]和subSeq[0,sp-1]。<br \>
	 * distance:不同的bit数。<br \>
	 * return Huffman(distance); 
	 * */
	private int MDL(int distance){
		return Huffman(distance);
	}	
	/**
	 * 以start为开始，遍历所有合法logSeq[start,x]，返回与freSeq[sp,length-1]最近的距离。
	 * */
	private int smallestDistance(int start,Meta[] freSeq,int sp){
		if(logSeq[start].equals(freSeq[sp]))
			return recursiveSmallestDistance(0,start,freSeq,sp);
		else
			return recursiveSmallestDistance(1,start,freSeq,sp);
	}
	private int recursiveSmallestDistance(int distance,int lastLogSeq,Meta[] freSeq,int lastFreSeq){
		//还剩下freSeq.length-1-lastFreSeq个Meta没匹配
		if(lastFreSeq+1==freSeq.length)//还剩下0个没匹配，返回
			return distance;
		int small = freSeq.length-1-lastFreSeq;
		for(int i=1;lastLogSeq+i<=logSeq.length-(freSeq.length-1-lastFreSeq);i++){//枚举每一个可能的下一logSeq元素
			if(logSeq[lastLogSeq+i].time-logSeq[lastLogSeq].time>gap)break;
			int tmp = 0;
			if(logSeq[lastLogSeq+i].equals(freSeq[lastFreSeq+1])){
				tmp = recursiveSmallestDistance(distance,lastLogSeq+i,freSeq,lastFreSeq+1);	
			}
			else{
				tmp = recursiveSmallestDistance(distance+1,lastLogSeq+i,freSeq,lastFreSeq+1);
			}			
			small = tmp<small?tmp:small;
		}
		return distance+small;
	}
}
