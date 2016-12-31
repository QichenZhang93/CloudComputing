import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Probability {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

		static enum CountersEnum { INPUT_WORDS }
    
		@Override
		public void setup(Context context) throws IOException,
        	InterruptedException {
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// INPUT: word[\s]word[\s]word...word[\t]Count
			System.err.println("Start mapping");
			String line = value.toString();
			
			String[] words = line.split("\\s+");
			
			if (words.length < 2) {
				return;
			}
			
			if (words.length == 2) {
				/*
				byte[] wordBytes;
				Text phaseText = new Text();
				Text wordCountText = new Text();
				wordBytes = words[0].trim().getBytes();
				phaseText.append(wordBytes, 0, wordBytes.length); // original phase
				wordBytes = words[1].trim().getBytes();
				wordCountText.append(wordBytes, 0, wordBytes.length);
				context.write(phaseText, wordCountText);
				*/
				return;
			}
			
			Integer phaseCount = Integer.valueOf(words[words.length - 1]);
			if (phaseCount <= 2) return;
			
			Text phaseText = new Text();
			Text wordCountText = new Text();
			for (int i = 0; i < words.length - 2; ++i) {
				
				byte[] wordBytes = words[i].trim().getBytes();
				phaseText.append(wordBytes, 0, wordBytes.length);
				phaseText.append(" ".getBytes(), 0, " ".getBytes().length);
			}
			
			byte[] wordBytes = words[words.length - 2].trim().getBytes();
			wordCountText.append(wordBytes, 0, wordBytes.length); // word
			wordCountText.append(" ".getBytes(), 0, " ".getBytes().length);
			wordBytes = words[words.length - 1].trim().getBytes();
			wordCountText.append(wordBytes, 0, wordBytes.length); // count
			phaseText.set(phaseText.toString().trim());
			context.write(phaseText, wordCountText);
			
		}
	}
	
	public static class WordCountPair {
		public String word;
		public Integer count;
		
		public WordCountPair(String w, Integer pr) {
			word = w;
			count = pr;
		}
	}
	
	// The priority queue should maintain the largest probability. If probabilities are equal,
	// the lexicographically smaller one should stay. e.g. bc should be popped before ab
	public static class WordProbPairComparator implements Comparator<WordCountPair> {

		@Override
		public int compare(WordCountPair o1, WordCountPair o2) {
			 if (o1.count.equals(o2.count)) {
				 return -o1.word.compareTo(o2.word);
			 }
			 return (o1.count.compareTo(o2.count));
		}
		
	}
	
	/* word[\s]word[\s]...word[\t]word[\s]count
	 * |--------phase--------|    |--|    |---|
	 * OR
	 * word[\s]word[\s]...word[\t]count
	 * |--------phase--------|    |---|
	 * 
	 * Hbase schema(prediction):
	 * ROW_KEY(phase)	COL_FAMILY(words)
	 * phase			KEY:word Value:probability
	 * 
	 * Pr(word|phase) = Count(phase + word) / Count(phase);
	 */
	public static class HbaseReducer extends TableReducer<Text, Text, NullWritable> {
		
		private Configuration conf;
	    
		private int gramNumber = 0;
    
		@Override
		public void setup(Context context) throws IOException,
        	InterruptedException {
			conf = context.getConfiguration();
			gramNumber = conf.getInt("gram.number", 5);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// key: phase
			// values: count or word[\s]count
			
			PriorityQueue<WordCountPair> priorityQueue = new PriorityQueue<>(gramNumber, new WordProbPairComparator());
			
			System.err.println("Start reducing");
			
			System.err.println("Key: " + key.toString());
			
			//Double phaseCount = 100.0;
			final String phase = key.toString();
			
			for (Text text : values) {
				String textString = text.toString();
				String[] words = textString.split("\\s+");
				if (words.length == 1) { // only one count
					//phaseCount = Double.valueOf(words[0]);
				}
				else {
					priorityQueue.add(new WordCountPair(words[0], Integer.valueOf(words[1])));
					while (priorityQueue.size() > 5) {
						priorityQueue.poll(); // pop the smallest one
					}
				}
			}
			
			if (priorityQueue.size() == 0) return; // no word after this phase --> so lonely!!
			
			Put put = new Put(phase.trim().getBytes());
			
			while (!priorityQueue.isEmpty()) {
				WordCountPair wordCountPair = priorityQueue.poll();
				// COL_FAMILY: words
				put.addColumn("words".getBytes(), wordCountPair.word.trim().getBytes(), String.valueOf((wordCountPair.count/* / phaseCount*/)).getBytes());
			}
			
			context.write(NullWritable.get(), put);
			
		}
	}
	
	public static class idleReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text text : values) {
				context.write(key, text);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		
		String zkAddr = "172.31.54.63";
		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
		
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		
		if ((remainingArgs.length != 1) && (remainingArgs.length != 3)) {
			System.err.println("Usage: wordcount <in> [-n gramNumber]");
			System.exit(2);
		}
    
		System.err.println("Start configuration");
		
		Job job = Job.getInstance(conf, "probability");
		job.setJarByClass(Probability.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(idleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setReducerClass(HbaseReducer.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i=0; i < remainingArgs.length; ++i) {
			if ("-n".equals(remainingArgs[i])) {
				++i;
				job.getConfiguration().setInt("gram.number", Integer.valueOf(remainingArgs[i]));
			}
			else {
				otherArgs.add(remainingArgs[i]);
			}
		}
		
		System.err.println("Input file: " + otherArgs.get(0));
		
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		
		TableMapReduceUtil.initTableReducerJob("prediction", HbaseReducer.class, job);
		
		System.err.println("Finish configuration");

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}