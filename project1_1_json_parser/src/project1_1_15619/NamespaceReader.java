package project1_1_15619;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Predicate;

import javax.naming.NoInitialContextException;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/*
 * Represent a single namespace in the given json file
 */
class NamespaceItem {
	public int Id;
	public String Name;
	public NamespaceItem() {
		Id = -2;
		Name = "";
	}
	@Override
	public String toString() {
		return ((Integer)Id).toString() + ' ' + Name;
	}
}

/*
 * A class to hold all excluded namespaces in the given json file
 */
class Namespaces {
	public ArrayList<NamespaceItem> Items;
	public Namespaces() {
		Items = new ArrayList<>();
	}
}

/*
 * A class to parse json file containing description of wiki namespace.
 */
public class NamespaceReader {
	
	protected static NamespaceItem ReadNamespaceItem(JsonReader reader) throws IOException {
		if (reader == null) return null;
		NamespaceItem nItem = null;
		reader.beginObject();
		while (reader.hasNext()) {
			String nxtName = reader.nextName();
			if (nxtName.equals("*")) {
				if (nItem == null) nItem = new NamespaceItem();
				nItem.Name = reader.nextString();
			}
			else if (nxtName.equals("id")) {
				if (nItem == null) nItem = new NamespaceItem();
				nItem.Id = reader.nextInt();
			}
			else reader.skipValue();
		}
		reader.endObject();
		return nItem;
	}
	
	protected static Namespaces ReadNamespaces(JsonReader reader) throws IOException {
		if (reader == null) return null;
		Namespaces namespaces = new Namespaces();
		reader.beginObject();
		while (reader.hasNext()) {
			String id = reader.nextName();
			if (id.equals("*")) {
				reader.skipValue();
			}
			else {
				NamespaceItem nItem = ReadNamespaceItem(reader);
				if (nItem == null) continue;
				namespaces.Items.add(nItem);
			}
		}
		reader.endObject();
		return namespaces;
	}
	
	protected static Namespaces ReadQuery(JsonReader reader) throws IOException {
		if (reader == null) return null;
		Namespaces namespaces = null;
		reader.beginObject();
		while (reader.hasNext()) {
			if (reader.nextName().equals("namespaces")) {
				namespaces = ReadNamespaces(reader);
				break;
			}
			else {
				reader.nextString();
			}
		}
		reader.endObject();
		return namespaces;
	}
	
	public static Namespaces ParseJsonFile(JsonReader reader) throws IOException {
		if (reader == null) return null;
		reader.beginObject();
		Namespaces namespaces = null;
		while (reader.hasNext()) {
			if (reader.nextName().equals("query")) {
				namespaces = ReadQuery(reader);
				break;
			}
			else {
				reader.nextString();
			}
		}
		reader.endObject();
		reader.close();
		
		// Remove the item whose id == 0
		/*
		namespaces.Items.removeIf(new Predicate<NamespaceItem>() {
			@Override
			public boolean test(NamespaceItem item) {
				return (item.Id == 0);
			}
		});*/
		
		return namespaces;
	}
	
	public static String GenerateCaseInsensitiveRegx(Namespaces namespaces) {
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append("^(");
		String tempStr = "";
		boolean isNotFirst = false;
		for (NamespaceItem nItem : namespaces.Items) {
			if (nItem.Id == 0) continue;
			if (isNotFirst) {
				sBuilder.append('|');
			}
			else isNotFirst = true;
			tempStr = nItem.Name;
			for (int i = 0; i < tempStr.length(); ++i) {	
				char c = tempStr.charAt(i);
				if (c == ' ') {
					sBuilder.append('_');
				}
				else if (c <= 122 && c >= 97) {
					sBuilder.append('[').append((char)(c - 32)).append(c).append(']');
				}
				else if (c >= 65 && c <= 90) {
					sBuilder.append('[').append((char)(c + 32)).append(c).append(']');
				}
				else {
					sBuilder.append(c);
				}
			}
		}
		sBuilder.append(')');
		sBuilder.append("((:)|(%3[Aa])).*");
		return sBuilder.toString();
	}
	
	/*
	 * The first arg in args is the path of json file that needs to be parsed
	 * The second arg in args is the output path. if null, the output will print the regx in console
	*/
	public static void main(String[] args) {
		if (args.length < 1) return;
		JsonReader reader;
		try {
			reader = new JsonReader( new FileReader(args[0]) );
		} catch (IOException e) {
			System.out.println("File: " + args[0] + " is not a valid path");
			return;
		}
		
		Namespaces namespaces;
		try {
			namespaces = NamespaceReader.ParseJsonFile(reader);
		} catch (IOException e) {
			System.out.println("File: " + args[0] + " is not a valid json file");
			return;
		}
		/*	
		for (NamespaceItem item : namespaces.Items) {
			System.out.println(item.toString() + '\n');
		}
		*/
		String regx = NamespaceReader.GenerateCaseInsensitiveRegx(namespaces);
		if (args.length < 2) System.out.println(regx);
		try {
			FileWriter fWriter = new FileWriter(args[1], false);
			fWriter.write(regx);
			fWriter.flush();
			fWriter.close();
			System.out.print(regx);
		} catch (IOException e) {
			System.out.println("File: " + args[1] + " is not a valid path");
		}
	}
}
