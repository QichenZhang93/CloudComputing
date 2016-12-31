import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class q9 {

	static int longestStrictlyDecreasingSequence(int[] array) {
		int maxLen = 0;
		int[] length = new int[array.length];
		for (int i = 0; i < length.length; ++i) {
			length[i] = 1;
		}
		maxLen = 1;
		for (int i = 1; i < length.length; ++i) {
			if (array[i] < array[i - 1]) {
				array[i] = array[i - 1] + 1;
				maxLen = maxLen < array[i] ? array[i] : maxLen;
			}
		}
		return maxLen;
	}
	
	static int[] ToIntArray(String[] strArray) {
		int[] intArray = new int[31];
		for (int i = 0; i < 31; ++i) {
			intArray[i] = Integer.parseInt(strArray[i + 2].substring(strArray[i + 2].lastIndexOf(":") + 1, strArray[i + 2].length()));
		}
		return intArray;
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		int lengthLongestDesSequence = 0;
		int sum = 0;
		String inputPath = args[0];
		Scanner scanner = new Scanner(new File(inputPath), "UTF-8");
		while (scanner.hasNextLine()) {
			String[] items = scanner.nextLine().split("\\s+"); // totalview articlename 1 2 3 ... 31
			int lds = longestStrictlyDecreasingSequence(ToIntArray(items));
			if (lds > lengthLongestDesSequence) {
				sum = 1; // reset
				lengthLongestDesSequence = lds;
			}
			else if (lds == lengthLongestDesSequence) {
				++sum;
			}
		}
		System.out.print(sum);
		scanner.close();
	}

}
