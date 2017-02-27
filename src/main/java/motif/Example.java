package motif;

import java.util.List;

public class Example {
	List<Meta[]> dimensions = null;//d-维 时间序列
	boolean label;//样本标记
	int example_id;//样本编号，区分不同样本
	public Example(List<Meta[]> dimensions,boolean label,int example_id){
		this.dimensions = dimensions;
		this.label =label;
		this.example_id = example_id;
	}
	@Override
	public String toString(){
		String s = "";
		s+=label?"+":"-"+" "+example_id+":";
		for(Meta[] ms:dimensions){
			s+="\n";
			for(Meta m:ms)s+=m+" ";
		}
		return s;
	}
}
