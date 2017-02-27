package motif;

import java.io.FileNotFoundException;
import java.util.*;

public class Main{
	public static void main(String args[]){
		MotifDiscovery md = new MotifDiscovery();
		List<Motif> motifs = null;
		Example[] examples = null;
		Map<Integer,Example> example_map = null;
		int dimension = 0;
		try {
			//数据预处理
			examples = MotifDiscovery.dataPreProcessing(MotifDiscovery.path);
			example_map = new HashMap<Integer,Example>();
			for(Example e:examples){
				example_map.put(e.example_id, e);
				System.out.println(e);
			}			
			//motif发现
			motifs = md.discover((float)0.5,(float)0.5,2,4,dimension,examples);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Over!");		
		
		RuleDiscovery rd = new DiscreteRuleDiscovery();
		for(Motif m:motifs){
			List<Meta[]> list = new ArrayList<Meta[]>();
			int size = 0;
			for(int example_id:m.indexMap.keySet()){
				Example e = example_map.get(example_id);
				if(e!=null&&e.label){
					list.add(e.dimensions.get(dimension));
					size+=e.dimensions.get(dimension).length;
				}
			}			
			//在两个序列结合的地方，要填一些占位符，避免找ac的时候引入前后连接的信息
			Meta[] T = new Meta[size];
			int i=0;
			for(Meta[] ml:list){
				for(Meta ma:ml){
					T[i] = ma;
					i++;
				}
			}
			Meta[] R = new Meta[m.seq.size()];
			for(i=0;i<m.seq.size();i++){
				R[i] = m.seq.get(i);
			}
			rd.findBestRules(T,R,64);
		}
		
		
	}	
}