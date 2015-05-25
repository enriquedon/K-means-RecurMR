import java.util.HashSet;
import java.util.Set;

public class test {

	public static void main(String[] args) {
		String a = "0.86	588.00	294.00	147.00	7.00	4	0.00	0	19.34	23.49";
		double[] rowb = rowToList(a);
		for (double d : rowb) {
			System.out.print(d + ",");
		}
	}

	private Set<Long> getRndCenter(int numOfclusters) {

		Set<Long> centers =new HashSet<Long>();
		for(int i=0;i<numOfclusters;i++){
			int rnd=(int)(Math.random()*768);
		}return centers;
	}

	private static double[] rowToList(String line) {
		String[] rowa = new String[10];
		double[] rowb = new double[10];
		rowa = line.split("	");
		for (int i = 0; i < rowa.length; i++) {
			// System.out.println(rowa[i]);
			rowb[i] = Double.parseDouble(rowa[i]);
		}
		return rowb;
	}
}
