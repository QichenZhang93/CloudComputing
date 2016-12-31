import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;

public class SortOutput {

	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
		ArrayList<String> arrayList = new ArrayList<>();
		String input;
		while ((input = reader.readLine()) != null) {
			arrayList.add(input);
		}
		arrayList.sort(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				String[] o1Arr = o1.split("\\s+");
				String[] o2Arr = o2.split("\\s+");
				int o1Int = Integer.parseInt(o1Arr[0]);
				int o2Int = Integer.parseInt(o2Arr[0]);
				if (o1Int == o2Int) {
					return o1Arr[1].compareTo(o2Arr[1]);
				}
				else {
					return -(o1Int - o2Int);
				}
			}
		});
		for (String string : arrayList) {
			System.out.println(string);
		}
	}

}
