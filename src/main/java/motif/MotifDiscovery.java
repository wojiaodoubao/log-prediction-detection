package motif;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class MotifDiscovery {
	public static final String[] path = {"C:\\Users\\jinglun\\Desktop\\example0","C:\\Users\\jinglun\\Desktop\\example1","C:\\Users\\jinglun\\Desktop\\example2","C:\\Users\\jinglun\\Desktop\\example3"};//每一个文件是一个example，每行是一个维度
	public List<Motif> discover(float POD,float FAR,int word_length,int motif_length,int dimension,Example[] examples) throws FileNotFoundException{
		if(examples==null||examples.length==0)return null;
		Map<Integer,Boolean> example_label = new HashMap<Integer,Boolean>();
		Map<Integer,Example> example_map = new HashMap<Integer,Example>();
		for(Example e:examples){
			example_label.put(e.example_id, e.label);
			example_map.put(e.example_id, e);
		}
		//构造trie树
		TrieTree tree = createTrie(examples,dimension,word_length);
		tree.rootFirstSearch(0);
		System.out.println();
		tree.rootLastSearch(0);
		//d-合法motif
		List<Motif> motifList = tree.leafNodeToMotif(example_label);
		for(Motif m:motifList)
			System.out.println(m);
		motifList = motifFilter(POD,FAR,motifList);
		//d-合法motif生成长motif
		List<Motif> finalMotifs = new ArrayList<Motif>();
		finalMotifs.addAll(motifList);
		while(word_length*2<=motif_length){
			List<Motif> tmpList = new ArrayList<Motif>();
			for(int i=0;i<motifList.size();i++){
				for(int j=i;j<motifList.size();j++){
					Motif m = Motif.mergeMotif(motifList.get(i),motifList.get(j),example_map,dimension);
					if(filter(POD,FAR,m))
						tmpList.add(m);
					if(i!=j)m=Motif.mergeMotif(motifList.get(j),motifList.get(i),example_map,dimension);
					if(filter(POD,FAR,m))
						tmpList.add(m);					
//					System.out.println("Merge:"+m);
				}
			}
			word_length *= 2;
			motifList = tmpList;
			finalMotifs.addAll(motifList);
		}
		//跨维度生成长motif
		//展示结果
		System.out.println("************结果*************");
		for(Motif m:finalMotifs)
			System.out.println(m);
		return finalMotifs;
	}
	private boolean filter(float POD,float FAR,Motif m){
		return m.POD>=POD&&m.FAR<=FAR;
	}
	private List<Motif> motifFilter(float POD,float FAR,List<Motif> seq){
		List<Motif> list = new ArrayList<Motif>();
		for(Motif m:seq){
			if(m.POD>=POD&&m.FAR<=FAR)
				list.add(m);
		}
		return list;
	}
	/**
	 * 每一个文件是一个example，每行是一个维度sequence
	 * 每个sequence以'\t'分割时间段，每个时间段跨度一个时间单位
	 * 
	 * 适合已经处理后规则的时间序列数据的预处理
	 * */
	public static Example[] dataPreProcessing(String[] paths) throws FileNotFoundException{
		Map<String,Meta> map = new HashMap<String,Meta>();
		long sid = 0;
		Example[] examples = new Example[path.length];
		for(int i=0;i<path.length;i++){
			Scanner sc = new Scanner(new FileInputStream(path[i]));
			List<Meta[]> dimensions = new ArrayList<Meta[]>();
			boolean label = sc.nextLine().equals("true");
			while(sc.hasNextLine()){//for each dimension
				String[] s = sc.nextLine().split("\t");
				Meta[] meta = new Meta[s.length];
				for(int j=0;j<s.length;j++){//for each meta
					Meta l = map.get(s[j]);
					if(l==null){
						l=new LogMeta(sid++);
						map.put(s[j], l);
					}
					meta[j] = l;
				}
				dimensions.add(meta);
			}
			sc.close();
			examples[i] = new Example(dimensions, label, i);
		}
		return examples;
	}
	private TrieTree createTrie(Example[] examples,int dimension,int length){
		TrieTree tree = new TrieTree();
		for(int i=0;i<examples.length;i++){
			 Meta[] seq = examples[i].dimensions.get(dimension);
			 for(int j=0;j<seq.length-length+1;j++){
				 int current = 0;
				 for(int l=0;l<length;l++)
					 current = tree.addChild(current, seq[j+l]);
				 tree.addIndexToNode(current, examples[i].example_id, j);
			 }
		}
		return tree;
	}
	private TrieTree createTrieEffective(Example[] examples,int dimension,int length){
		TrieTree tree = new TrieTree();
		return tree;
	}
}
