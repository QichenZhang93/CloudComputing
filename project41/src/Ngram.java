import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xml.sax.SAXException;

import javax.xml.parsers.*;
import org.w3c.dom.*;

public class Ngram {
	
	private final static String regex_attr_xml = "[a-zA-Z_:][-a-zA-Z0-9_:.]*";
	public static String handleContent(String line) throws ParserConfigurationException, SAXException, IOException {
		/*
	   	 * Replace all the URLs with single space because they are not part of human language. Using this regex:
	   	  	(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]
	   	 * Replace all the <ref> and </ref> tags with single space. Notice that there might be attributes in the tag. For example, <ref class=xxx>.
	   	 * Please turn all of your words and phrases into lower-case
	   	 * Keep apostrophes that are part of a word. In other words, don't remove apostrophes that are surrounded by letters.
	   	 * Replace other non-alphabetic characters with single space.
	   	 * Merge continuous spaces into single space so that you won't have empty words or phrases
	   	 */
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder;
		documentBuilder = documentBuilderFactory.newDocumentBuilder();
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(line.getBytes("UTF-8"));
		Document document;
		document = documentBuilder.parse(byteArrayInputStream);
		NodeList textTags = document.getElementsByTagName("text");
		Node textTag = textTags.item(0);
		
		String content = textTag.getTextContent();
		
		//System.out.println(content);
		
		content = content.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ")
				.replaceAll(String.format("<ref((\\s+)(%s)=(('[^']*')|(\"[^\"]*\"))(\\s)*)*/?>|</ref>", regex_attr_xml), " ")
				.toLowerCase()
				.replaceAll("[^a-z']+", " ")
				.replaceAll("[^a-z]+'|'[^a-z]+|^'|'$", " ")
				.replaceAll("\\s+", " ")
				.trim();
				;
		
		return content;
	}

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String line = value.toString(); //.toLowerCase();
    	try {
			line = handleContent(line);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			return;
		} catch (SAXException e) {
			return;
		}
    	
    	StringTokenizer itr = new StringTokenizer(line);
    	/*
    	 * TODO: 1 - 5 grams
    	 */
    	LinkedList<String> bufferedStrings = new LinkedList<String>();
      	while (itr.hasMoreTokens()) {
      		
      		bufferedStrings.add(itr.nextToken());
      		
      		int bufferedStringsSize = bufferedStrings.size();
      		
      		if (bufferedStringsSize > 5) {
      			bufferedStrings.pop();
      		}
      		
      		Iterator<String> rIterator = bufferedStrings.descendingIterator();
      		LinkedList<String> tempStrings = new LinkedList<String>();
      		
      		while (rIterator.hasNext()) {
      			//StringBuilder word = new StringBuilder();
      			word.clear();
      			
      			tempStrings.push(rIterator.next());
      			boolean isFirstToken = true;
      			
      			for (String string : tempStrings) {
      				if (!isFirstToken) {
      					byte[] strBytes = " ".getBytes("UTF-8");
      					word.append(strBytes, 0, strBytes.length);
      				}
      				else {
      					isFirstToken = false;
      				}
      				byte[] strBytes = string.getBytes("UTF-8");
      				word.append(strBytes, 0, strBytes.length);
    			}
      			
      			context.write(word, one);
      			//System.out.println(word.toString() + " " + 1);
      		}
      	}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	/**
    	 * You may want to ignore words or phrases that appear below a certain threshold, say t, 
    	 * to limit the size of your output. Use t = 2 
    	 * (This means that you need to ignore the words or phrases whose count is less than or equal to 2).
    	 */
    	int sum = 0;
    	for (IntWritable val : values) {
    		sum += val.get();
    	}
    	if (sum <= 2) return;
    	result.set(sum);
    	context.write(key, result);
    }
    
  }

  public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Ngram.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
	  /*
	  String xmlString = "<page><revision><id>732358840</id><parentid>704917736</parentid><timestamp>2016-07-31T12:29:53Z</timestamp><contributor><username>Colonies Chris</username><id>577301</id></contributor><minor /><comment>minor fixes using [[Project:AWB|AWB]]</comment><model>wikitext</model><format>text/x-wiki</format><text xml:space=\"preserve\">'sdf'''''sdf{{UStheater}}There are many famous '''theat-d-'__--ers in [[Louisiana]]'asddas'', most notably in [[New Orleans]].==Abbeville==* [[Abbey Players (Abbeville)|Abbey Players]]==Alexandria==* [[Coughlin-Saunders Performing Arts Center]]* [[Hearn Stage at The Kress Theatre]]* [[Rapides Opera House]]==Baton Rouge==* [[Baton Rouge River Center Theater for Performing Arts]]* [[Greek Theatre (Baton Rouge)|Greek Theatre]]* [[Reilly Theatre]]* [[Shaw Center for the Arts Manship Theatre]]==Hammond==* [[Columbia Theatre for the Performing Arts]]==Metairie==* [[Jefferson Performing Arts Center (Louisiana)|Jefferson Performing Arts Center]]==New Orleans==* [[Anthony Bean Community Theater]]* [[Carver Theater (New Orleans)|Carver Theater]]* [[Civic Theatre (New Orleans)|Civic lkjlaksjfldskfjdslkfjdslkfjdslkjfdslkj'sdlkfjslkdjflksjd Theatre]]* [[Joy Theater]]* [[Le Petit Theatre du Vieux Carre]]* [[Mahalia Jackson Theater of the Performing Arts]]* [[Orpheum Theatre (New Orleans)|Orpheum Theatre]]* [[Saenger Theatre (New Orleans)|Saenger Theatre]]* [[State Palace Theatre (New Orleans)|State Palace Theatre]]==Shreveport==* [[RiverView Theater (Shreveport, Louisiana)|RiverView Theater]]* [[Shreveport Municipal Memorial Auditorium]]==St. Martinville==* [[Duchamp Opera House]]==Thibodaux==* [[Thibodaux Playhouse, Inc.]]{{coord missing|Louisiana}}[[Category:Theatres in Louisiana]]{{theat-stub}} &lt;ref consaa=\"asd\" ssse_0='sdf' &gt;{{cite web|title=Major Waterfalls|url=http://www.keralatourism.org/wayanad/waterfalls-wayanad.php}} &lt;/ref&gt; aa'''bb'c'</text><sha1>7z66ggf2xllinij2krsh385bz0wbh6v</sha1></revision></page>";
	  xmlString = handleContent(xmlString);
	  System.out.println(xmlString);
	  
	  StringTokenizer itr = new StringTokenizer(xmlString);

  	LinkedList<String> bufferedStrings = new LinkedList<String>();
  	
  	while (itr.hasMoreTokens()) {
  		
  		bufferedStrings.add(itr.nextToken());
  		
  		int bufferedStringsSize = bufferedStrings.size();
  		
  		if (bufferedStringsSize > 5) {
  			bufferedStrings.pop();
  		}
  		
  		Iterator<String> rIterator = bufferedStrings.descendingIterator();
  		LinkedList<String> tempStrings = new LinkedList<String>();
  		
  		while (rIterator.hasNext()) {
  			StringBuilder word = new StringBuilder();
  			tempStrings.push(rIterator.next());
  			boolean isFirstToken = true;
  			
  			for (String string : tempStrings) {
  				if (!isFirstToken) {
  					//byte[] strBytes = " ".getBytes("UTF-8");
  					word.append(" ");
  				}
  				else {
  					isFirstToken = false;
  				}
  				word.append(string);
			}
  			
  			System.out.println(word.toString() + " " + 1);
  		}
  	}*/
	  
	  //String string = "<ref   consaa=\"asd\"  ssse_0='sdf'   />{{cite web|title=Major Waterfalls|url=http://www.keralatourism.org/wayanad/waterfalls-wayanad.php}} </ref>".replaceAll(String.format("<ref((\\s+)(%s)=(('[^']*')|(\"[^\"]*\"))(\\s)*)*/?>|</ref>", regex_attr_xml), " ");
	  //System.out.println(string);
	  
  }
}