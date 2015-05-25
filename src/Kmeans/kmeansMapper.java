package Kmeans;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

public class kmeansMapper extends TableMapper<Text, Text> {
	Text outputkey = new Text();
	Text outputvalue = new Text();
	int i = 0;

	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		int LogType = Integer.valueOf(new String(value.getValue(
				"_0".getBytes(), "lgtp".getBytes())));
//		if (LogType == PLAYER_LOGINZONE || LogType == PLAYER_LOGOUT) {
//			String key = new String(value.getValue("_0".getBytes(),
//					"area".getBytes()))
//					+ DEFAULT_DELIMITER
//					+ ("00" + new String(value.getValue("_0".getBytes(),
//							"wid".getBytes()))).substring(1)
//					+ DEFAULT_DELIMITER
//					+ new String(value.getValue("_0".getBytes(),
//							"pid".getBytes()));
//			outputkey.set(key);
//			String val = new String(value.getValue("_0".getBytes(),
//					"uts".getBytes()))
//					+ DEFAULT_DELIMITER + LogType;
//			outputvalue.set(val);
//			context.write(outputkey, outputvalue);
//			i++;
//		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		context.getCounter("CCU", "Mapper Count").setValue(i);
	};
}