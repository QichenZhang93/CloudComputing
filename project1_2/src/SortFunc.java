import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;

public class SortFunc {

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
				return o1.compareTo(o2);
			}
		});
		for (String string : arrayList) {
			System.out.println(string);
		}
	}
}
