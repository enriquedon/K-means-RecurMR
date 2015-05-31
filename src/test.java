import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class test {

	public static void main(String[] args) {
		System.out.print(randInt(0,13));
	}

	private Set<Long> getRndCenter(int numOfclusters) {

		Set<Long> centers = new HashSet<Long>();
		for (int i = 0; i < numOfclusters; i++) {
			int rnd = (int) (Math.random() * 768);
		}
		return centers;
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

	public static int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}
