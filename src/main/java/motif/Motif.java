package motif;

import java.util.*;
import java.util.Map.Entry;

public class Motif {
	List<Meta> seq;
	float POD;// POD=TP/(TP+FN) 此motif预测对正例占所有正例的比例 越高越好最高100%
	float FAR;// FAR=FP/(TP+FP) 此motif预测为正的有多少预测错     越低越好最低0%
	float CSI;// CSI=TP/(TP+FP+FN) TP+FN+FP=T+FP 因为永远预测为P，所以TN=0，故CSI=(TP+TN)/(TP+FP+FN+TN)预测正确率
	Map<Integer,List<Integer>> indexMap;//序列出现标记位置索引-<example_id,位置索引>
	public Motif(Meta[] ms,Map<Integer,List<Integer>> indexMap,Map<Integer,Boolean> example_label){
		this.indexMap = indexMap;
		seq = new ArrayList<Meta>();
		for(Meta m:ms)
			seq.add(m);
		int T = 0;//正例
		int F = 0;//负例
		for(boolean label:example_label.values()){
			if(label)T++;
			else F++;
		}
		int TP = 0;
		int FP = 0;
		int FN = 0;
		int TN = 0;
		for(Entry<Integer,List<Integer>> entry:indexMap.entrySet()){
			boolean label = example_label.get(entry.getKey());
			if(label)
				TP += entry.getValue().size();
			else
				FP += entry.getValue().size();
		}
		POD = T==0?0:TP/(float)T;
		FAR = (TP+FP)==0?0:FP/(float)(TP+FP);
		CSI = TP/(float)(T+FP);
	}
	private static Map<Integer,Boolean> exampleToLabel(Map<Integer,Example> example_map){
		Map<Integer,Boolean> example_label = new HashMap<Integer,Boolean>();
		for(Entry<Integer,Example> entry:example_map.entrySet()){
			example_label.put(entry.getKey(), entry.getValue().label);
		}
		return example_label;
	}
	public static Motif mergeMotif(Motif first,Motif second,Map<Integer,Example> example_map,int dimension){
		Meta[] ms = new Meta[first.seq.size()+second.seq.size()];
		for(int i=0;i<first.seq.size()+second.seq.size();i++){
			if(i<first.seq.size())
				ms[i] = first.seq.get(i);
			else
				ms[i] = second.seq.get(i-first.seq.size());
		}
		Map<Integer,List<Integer>> indexMap = new HashMap<Integer,List<Integer>>();
		for(Entry<Integer,List<Integer>> entry:first.indexMap.entrySet()){
			Meta[] m = example_map.get(entry.getKey()).dimensions.get(dimension);
			for(Integer i:entry.getValue()){
				int j = 0;
				for(j=0;j<second.seq.size()&&i+first.seq.size()+j<m.length;j++)
					if(!m[i+first.seq.size()+j].equals(second.seq.get(j)))break;
				if(j==second.seq.size()){
					List<Integer> list = indexMap.get(entry.getKey());
					if(list==null){
						list = new ArrayList<Integer>();
						indexMap.put(entry.getKey(), list);
					}
					list.add(i);
				}
			}
		}
		return new Motif(ms,indexMap,exampleToLabel(example_map));
	}
	@Override
	public String toString(){
		String s = "POD:"+POD+" FAR:"+FAR+" CSI:"+CSI;
		for(Meta m:seq)
			s+=m;
		return s;
	}
}
