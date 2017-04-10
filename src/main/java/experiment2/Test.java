package experiment2;

import java.io.*;

public class Test {
	public static void main(String args[]) throws IOException{
		String dir = TestDataGenerator.directory+"/seq-data";
		BufferedReader br = new BufferedReader(new FileReader(dir));
		String x = "1:";
		String s = null;
		while((s=br.readLine())!=null){
			if(s.indexOf(x)==0)
				System.out.println(s);
		}
		br.close();
	}
}
