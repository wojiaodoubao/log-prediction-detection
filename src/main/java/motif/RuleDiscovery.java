package motif;

import java.util.*;

public abstract class RuleDiscovery {
	/**
	 * T:时间序列 R:T的一个子序列
	 * */
	public void findBestRules(Meta[] T,Meta[] R,int cardinality){
		//计算表示一个Meta需要的bit数
		double r = (Math.log(cardinality)/Math.log(2.0));//log2(cardinality)，表示这些Meta需要多少位！
		int bit = 0;
		if(r-(int)r==0)bit = (int)r;
		else bit = 1+(int)r;
		System.out.println("bit:"+bit);
		//结果记录
		int best_sp = -1;
		int best_total = -1;
		int best_n = -1;
		//枚举划分点
		for(int sp=1;sp<R.length;sp++){//R[0,sp-1]->R[sp,R.length-1]
			List<Integer> ac = findAntecedentCandidates(T,R,sp);
			//find_Best_Number_of_Rule_Instances
			int total = 0;
			int n = 0;
			for(n=0;n<ac.size();n++){
				int subConsequentBits = Huffman(T,ac.get(n)+sp,ac.get(n)+R.length-1,bit);
				int subConsequentMDLbits = MDL(T,ac.get(n)+sp,ac.get(n)+R.length-1,R,sp,R.length-1,bit);
				System.out.println(subConsequentBits+" "+subConsequentMDLbits);
				showSequence(T,ac.get(n)+sp,ac.get(n)+R.length-1);
				showSequence(R,sp,R.length-1);
				if(subConsequentBits-subConsequentMDLbits<=0)break;
				total+=subConsequentBits-subConsequentMDLbits;
			}
			total -= Huffman(R,sp,R.length-1,bit);
			//在sp=sp时的totalBitSave和ac-n
			//total是totalBitSave，n是用到的ac中的子序列数目
			if(total>best_total){
				best_sp = sp;
				best_total = total;
				best_n = n;
			}
			System.out.println(sp+" "+total+" "+n);
		}
		//循环结束找到bestTotalBitSave对应的sp，和此时的total,ac-n
		System.out.println("*******结果*******");
		String result = "规则:";
		for(int i=0;i<R.length;i++){
			if(i==best_sp)
				result+="->";
			result+=R[i];
		}
		result+="\nbest_total:"+best_total+"\nbest_n:"+best_n;
		System.out.println(result);
	} 
	private int Huffman(Meta[] T,int x,int y,int bit){//就是DL()函数，编码T[x-y]需要的二进制码位数
		return (y-x+1)*bit;//cardinality设置为2^64。
	}
	private int MDL(Meta[] T,int Tl,int Tr,Meta[] R,int Rl,int Rr,int bit){
		int sum = 0;
		for(int i=0;i<=Tr-Tl;i++){
			if(!T[Tl+i].equals(R[Rl+i]))sum++;
		}
		return sum*bit;
	}
	/**
	 * 对于T中所有长为sp的子序列，按照与R[0~sp-1]的距离由小到大，返回子序列在T中的索引
	 * */
	private List<Integer> findAntecedentCandidates(Meta[] T,Meta[] R,int sp){//R[0~sp-1]
		class AntecedentAndDistance implements Comparable{
			int index;
			double distance;
			public AntecedentAndDistance(int index,double distance){
				this.index = index;
				this.distance = distance;
			}
			public int compareTo(Object o) {
				if(this == o)
					return 0;
				else if(o!=null && o instanceof AntecedentAndDistance){
					AntecedentAndDistance aad = (AntecedentAndDistance)o;
					if(distance<aad.distance)
						return -1;
					else if(distance>aad.distance)
						return 1;
					else
						return 0;
				}
				else
					return 1;
			}
		}
		List<AntecedentAndDistance> tmpList = new ArrayList<AntecedentAndDistance>();
		List<Integer> result = new ArrayList<Integer>();
		if(T==null||R==null||T.length<=sp)
			return result;
		for(int i=0;i<=T.length-R.length;i++){
			tmpList.add(new AntecedentAndDistance(i,distance(T,i,i+sp-1,R,0,sp-1)));
		}
		Collections.sort(tmpList);
		for(int i=0;i<tmpList.size()-1;i++)
			result.add(tmpList.get(i).index);
		return result;
	}
	public abstract double distance(Meta[] a,int al,int ar,Meta[] b,int bl,int br);
	private void showSequence(Meta[] T,int start,int end){
		if(start>end||end>=T.length||start<0)return;
		String s = "";
		while(start<=end){
			s+=T[start++];
		}
		System.out.println(s);
	}
}
class DiscreteRuleDiscovery extends RuleDiscovery{

	@Override
	public double distance(Meta[] a, int al, int ar, Meta[] b, int bl, int br) {
		if(a==null||b==null||ar<al||br<bl||al<0||ar<0||bl<0||br<0||ar>=a.length||br>-b.length||(ar-al)!=(br-bl))return -1;
		int sum = 0;
		for(int i=0;i<ar-al+1;i++)
			if(!a[al+i].equals(b[bl+i]))sum++;
		return ((double)sum)/(br-bl+1);
	}
	
}
