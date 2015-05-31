//package Kmeans;
//
//import java.io.IOException;
//
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//
//import Program.globalNameSpace;
//
//public class kmeansCombiner extends TableReducer<IntWritable, Text,ImmutableBytesWritable> {
//
//	double[] newCenter = new double[numOfColumn];
//	String[] newCenterInString = new String[numOfColumn];
//	private static int numOfrow;
//
//	public void reduce(IntWritable key, Iterable<Text> values, Context context)
//			throws IOException, InterruptedException {
//		numOfrow = 0;
//		for (Text val : values) {
//			numOfrow++;
//			calNewCenter(val.toString());
//		}
//		assignNewCenterIntoString();
//		System.out.println("in combiner numOfrow: " + numOfrow);
//		ImmutableBytesWritable HKey = new ImmutableBytesWritable(
//				Bytes.toBytes(key.toString()));
//		Put HPut = new Put(Bytes.toBytes(key.toString()));
//		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x1),
//				Bytes.toBytes(newCenterInString[0]));
//		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x5),
//				Bytes.toBytes(newCenterInString[1]));
//		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(x6),
//				Bytes.toBytes(newCenterInString[2]));
//		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y1),
//				Bytes.toBytes(newCenterInString[3]));
//		HPut.add(Bytes.toBytes(Family1), Bytes.toBytes(y2),
//				Bytes.toBytes(newCenterInString[4]));
//
//		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x2),
//				Bytes.toBytes(newCenterInString[5]));
//		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x3),
//				Bytes.toBytes(newCenterInString[6]));
//		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x4),
//				Bytes.toBytes(newCenterInString[7]));
//		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x7),
//				Bytes.toBytes(newCenterInString[8]));
//		HPut.add(Bytes.toBytes(Family2), Bytes.toBytes(x8),
//				Bytes.toBytes(newCenterInString[9]));
//		context.write(HKey, HPut);
//	}
//
//	private void calNewCenter(String dataString) {
//		String[] curDataInString = new String[numOfColumn];
//		double[] curData = new double[numOfColumn];
//		curDataInString = dataString.split("	");
//		for (int i = 0; i < numOfColumn; i++) {
//			newCenter[i] += Double.parseDouble(curDataInString[i]);
//		}
//	}
//
//	private void assignNewCenterIntoString() {
//		System.out.println(numOfrow);
//		for (int i = 0; i < numOfColumn; i++) {
//			newCenter[i] /= numOfrow;
//			newCenterInString[i] = Double.toString(newCenter[i]);
//		}
//	}
////	public class kmeansCombiner extends TableReducer<IntWritable, Text,IntWritable> {
////
////		double[] newCenter = new double[numOfColumn];
////		String[] newCenterInString = new String[numOfColumn];
////		private static int numOfrow;
////		String dataString;
////
////		public void reduce(IntWritable key, Iterable<Text> values, Context context)
////				throws IOException, InterruptedException {
////			numOfrow = 0;
////			for (Text val : values) {
////				numOfrow++;
////				calNewCenter(val.toString());
////			}
////			assignNewCenterIntoString();
////			System.out.println("in combiner numOfrow: " + numOfrow);
////			context.write(key, new Text(dataString));
////		}
////
////		private void calNewCenter(String dataString) {
////			String[] curDataInString = new String[numOfColumn];
////			double[] curData = new double[numOfColumn];
////			curDataInString = dataString.split("	");
////			for (int i = 0; i < numOfColumn; i++) {
////				newCenter[i] += Double.parseDouble(curDataInString[i]);
////			}
////		}
////
////		private void assignNewCenterIntoString() {
////			System.out.println(numOfrow);
////			for (int i = 0; i < numOfColumn; i++) {
////				newCenter[i] /= numOfrow;
//////				newCenterInString[i] = Double.toString(newCenter[i]);
////				dataString+=Double.toString(newCenter[i])+"	";
////			}
////		}
//	private static final int numOfColumn = globalNameSpace.numOfColumn;
//	private static final String Family1 = globalNameSpace.Family1;
//	private static final String Family2 = globalNameSpace.Family2;
//	public static final String x1 = globalNameSpace.x1;
//	public static final String x2 = globalNameSpace.x2;
//	public static final String x3 = globalNameSpace.x3;
//	public static final String x4 = globalNameSpace.x4;
//	public static final String x5 = globalNameSpace.x5;
//	public static final String x6 = globalNameSpace.x6;
//	public static final String x7 = globalNameSpace.x7;
//	public static final String x8 = globalNameSpace.x8;
//	public static final String y1 = globalNameSpace.y1;
//	public static final String y2 = globalNameSpace.y2;
//}
