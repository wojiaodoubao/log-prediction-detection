package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
//		return Long.parseLong(this.time.toString())<Long.parseLong(lw.time.toString())?-1:1;
		SimpleDateFormat sdf = new SimpleDateFormat(StaticInfo.DATE_STRING);
		try {
			Date dateThis = sdf.parse(this.time.toString());
			Date dateLW = sdf.parse(lw.time.toString());
			return dateThis.compareTo(dateLW);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;			
	}
	@Override
	public int hashCode(){
		return content.hashCode();
	}
}