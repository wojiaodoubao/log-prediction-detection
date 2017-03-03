package singlenode;

import java.io.FileNotFoundException;
import java.util.*;

import motif.Example;
import motif.Meta;
import motif.Motif;

public class SingleNodeRuleGegeration{
	public static final String[] windows_path = {
			"C:\\Users\\jinglun\\Desktop\\example0",
			"C:\\Users\\jinglun\\Desktop\\example1",
			"C:\\Users\\jinglun\\Desktop\\example2",
			"C:\\Users\\jinglun\\Desktop\\example3"};//每一个文件是一个example，每行是一个维度
	public static final String[] linux_path = {
			"/media/belan/windows/Users/jinglun/Desktop/example0",
			"/media/belan/windows/Users/jinglun/Desktop/example1",
			"/media/belan/windows/Users/jinglun/Desktop/example2",
			"/media/belan/windows/Users/jinglun/Desktop/example3"};	
	public static void main(String args[]) throws FileNotFoundException{
		MotifDiscovery md = new MotifDiscovery();
		List<Motif> motifs = null;
		Example[] examples = null;
		Map<Integer,Example> example_map = null;
		int dimension = 0;
		//Data Preprocessing
		Properties props = System.getProperties();
		String[] path = null;
		if(props.getProperty("os.name").equals("Linux"))
			path = linux_path;
		else if(props.getProperty("os.name").equals("Windows"))
			path = windows_path;
		else{
			System.out.println("没有文件路径");
			return;
		}
		examples = MotifDiscovery.dataPreProcessing(path);
		example_map = new HashMap<Integer,Example>();
		for(Example e:examples){
			example_map.put(e.getExampleID(), e);
		}
		System.out.println("*******Display Examples**********");
		printExamplesToConsole(examples);//输出examples到控制台
		//Motif discover
		motifs = md.discover((float)0.5,(float)0.5,2,4,dimension,examples);
		//print Motif discover results
		System.out.println("************Motif Discover Result*************");
		for(Motif m:motifs)
			System.out.println(m);	
		System.out.println("Motif Discover Finish!");		
		
		RuleDiscovery rd = new DiscreteRuleDiscovery();
		for(Motif m:motifs){
			List<Meta[]> list = new ArrayList<Meta[]>();
			int size = 0;
			for(int example_id:m.indexMap.keySet()){
				Example e = example_map.get(example_id);
				if(e!=null&&e.getLabel()){
					list.add(e.getDimensions().get(dimension));
					size+=e.getDimensions().get(dimension).length;
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
	private static void printExamplesToConsole(Example[] examples){
		for(Example e:examples){
			System.out.println(e);
		}		
	}
}