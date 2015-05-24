//import java.io.IOException;
//
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapred.TableOutputFormat;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Mapper;
//
//public class Imports {
//	public static final String NAME = "ImportFromFile";
//
//	public enum Counters {
//		LINES
//	}
//
//	static class ImportMapper extends
//			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
//		private byte[] family = null;
//		private byte[] qualifier = null;
//
//		@Override
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			String column = context.getConfiguration().get("conf.column");
//			byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
//			family = colkey[0];
//			if (colkey.length > 1) {
//				qualifier = colkey[1];
//			}
//		}
//
//		@Override
//		public void map(LongWritable offset, Text line, Context context)
//				throws IOException {
//			try {
//				String lineString = line.toString();
//				byte[] rowkey = DigestUtils.md5(lineString);
//				Put put = new Put(rowkey);
//				put.add(family, qualifier, Bytes.toBytes(lineString));
//				context.write(new ImmutableBytesWritable(rowkey), put);
//				context.getCounter(Counters.LINES).increment(1);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		Configuration conf = HBaseConfiguration.create();
//		conf.set("conf.column", column);
//		Job job = new Job(conf, "Import from file " + input + " into table "
//				+ table);
//
//		job.setMapperClass(ImportMapper.class);
//		job.setOutputFormatClass(TableOutputFormat.class);
//		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
//		job.setOutputKeyClass(ImmutableBytesWritable.class);
//		job.setOutputValueClass(Writable.class);
//		job.setNumReduceTasks(0);
//		FileInputFormat.addInputPath(job, new Path(input));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}
//}