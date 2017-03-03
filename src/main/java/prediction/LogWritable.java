package prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogWritable implements WritableComparable<LogWritable>{
	public Text content = new Text("");
	public Text logFile = new Text("");
	public Text time = new Text("");
	public LogWritable(){
		
	}
	public LogWritable(String content,String logFile,String time){
		this.content.set(content);
		this.logFile.set(logFile);
		this.time.set(time);
	}
	public void write(DataOutput out) throws IOException {
		this.content.write(out);
		this.logFile.write(out);
		this.time.write(out);
	}
	public void readFields(DataInput in) throws IOException {
		this.content.readFields(in);
		this.logFile.readFields(in);
		this.time.readFields(in);
	}
	//完全排序，先比content，再比logFile，最后比time。
	public int compareTo(LogWritable lw) {
		int res = this.content.compareTo(lw.content);
		if(0!=res)return res;
		res = this.logFile.compareTo(lw.logFile);
		if(0!=res)return res;
		return this.time.compareTo(lw.time);			
	}
	@Override
	public int hashCode(){
		return content.hashCode();
	}
}