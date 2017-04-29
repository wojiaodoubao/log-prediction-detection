package log;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	
	protected static final Log LOG = LogFactory.getLog(LogMapper.class.getName());
	
	/**
	* Called once at the beginning of the task.
	*/
	protected void setup(Context context
	) throws IOException, InterruptedException {
		// log info
		LOG.info("Mapper setup!");
		super.setup(context);
	}
	
	/**
	* Called once for each key/value pair in the input split. Most applications
	* should override this, but the default is the identity function.
	*/
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, 
	Context context) throws IOException, InterruptedException {
		//log info
		LOG.info("Mapper map!");
		//super.map(key, value, context);
	}
	
	/**
	* Called once at the end of the task.
	*/
	protected void cleanup(Context context
	) throws IOException, InterruptedException {
		//log info
		LOG.info("Mapper cleanup!");
		super.cleanup(context);
	}
	
	/**
	* Expert users can override this method for more complete control over the
	* execution of the Mapper.
	* @param context
	* @throws IOException
	*/
	public void run(Context context) throws IOException, InterruptedException {
		//log info
		super.run(context);
	}	
}
