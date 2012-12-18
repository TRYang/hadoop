import java.lang.Character;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import org.htmlparser.Parser;
import org.htmlparser.beans.StringBean;
import org.htmlparser.util.ParserException;

public class PageCluster {
	
	/*
	 * 每行输入一个html文件，先用htmlparser抓取正文内容，然后用Separator（中文分词器）分词
	 * <key, value> = <文件的id(用原文件的偏移量）+ 词组, 1>
	 */
	public static class getHtmlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private Text OutputKey = new Text();
		private final static IntWritable one = new IntWritable(1);

		public static String translate(String Text)
			    throws ParserException {
			StringBean sb = new StringBean();
			Parser parser = Parser.createParser(Text, "utf-8");
			parser.visitAllNodesWith(sb);
			return sb.getStrings();
		}
		
		public void setup(Context context) throws IOException {
			Path []caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches == null || caches.length <= 0) {
				System.err.print("Not found dictionary.");
				System.exit(2);
			}
			BufferedReader word_reader = new BufferedReader(new FileReader(caches[0].toString()));
			BufferedReader stopword_reader = new BufferedReader(new FileReader(caches[1].toString()));
            
			Separator.InitialDictionary(word_reader, stopword_reader);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String originText = value.toString();
			try {
				String text = translate(originText);
				String [] textList = text.split("\n");
				String [] word;
				StringTokenizer tokenizer;
				String buf;
				for (int i = 0; i < textList.length; i++) {
					tokenizer = new StringTokenizer(textList[i]);
					while (tokenizer.hasMoreTokens()) {
						buf = tokenizer.nextToken();
						if (buf.length() > 4) {
							buf = Separator.Split(buf);
						} else {
							while (buf.length() > 0 && Character.isDigit(buf.charAt(0))) buf = buf.substring(1);
							while (buf.length() > 0 && Character.isDigit(buf.charAt(buf.length() - 1))) buf = buf.substring(0, buf.length() - 1);
						}
						if (buf.length() <= 0) continue;
						word = buf.split(" ");
						for (String itr: word) {
							if (itr.length() <= 1) continue;
							boolean flag = false;
							for (int j = 0; j < itr.length(); j++)
								if (!Character.isDigit(itr.charAt(j))) {
									flag = true;
									break;
								}
							if (flag) {
								OutputKey.set(String.valueOf(key.get()) + " " + itr);
								context.write(OutputKey, one);
							}
						}
					}
				}
			}
			catch (Exception e) {
				System.err.print("html file is not legal");
				System.exit(1);
			}
		}
		
	}
	
	/*
	 * 把同一个key的求和
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final static int kLeastOccur = 4;
		private IntWritable ret_value = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int result = 0;
			for (IntWritable it: values) {
				result += it.get();
			}
			if (result >= kLeastOccur) {
				ret_value.set(result);
				context.write(key, ret_value);
			}
		}
	}
	
	/*
	 * 统计文档词的总数
	 */
	public static class getDocumentNameMapper extends Mapper<Text, Text, LongWritable, IntWritable> {
		private static LongWritable number = new LongWritable();
		private static IntWritable OutputValue = new IntWritable();
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String [] text = key.toString().split(" ");
			int tv = Integer.parseInt(value.toString());
			number.set(Long.parseLong(text[0]));
			OutputValue.set(tv);
			context.write(number, OutputValue);
		}
	}
	
	/*
	 * 对同一个网页的单词数求和
	 */
	public static class DocumentWordSumReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		private static int totalDocument = 0;
		private IntWritable ret_value = new IntWritable();
		
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int result = 0;
			for (IntWritable it: values) {
				result += it.get();
			}
			ret_value.set(result);
			context.write(key, ret_value);
			++totalDocument;
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			LongWritable key = new LongWritable(-1);
			IntWritable value = new IntWritable(totalDocument);
			context.write(key, value);
		}
	}
	
	/*
	 * 将输入的<文档，单词>转换成<单词,1>
	 */
	public static class getWordMapper extends Mapper<Text, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static Text OutputKey = new Text();
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String [] word = key.toString().split(" ");
			OutputKey.set(word[1]);
			context.write(OutputKey, one);
		}
	}
	
	/*
	 * 把同一个单词的value求和
	 */
	public static class WordSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable ret_value = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int result = 0;
			for (IntWritable it: values) {
				result += it.get();
			}
			ret_value.set(result);
			context.write(key, ret_value);
		}
	}
	
	/*
	 * 计算tf * idf
	 */
	public static class calcTFIDFMapper extends Mapper<Text, Text, Text, Text> {
		private TreeMap<String, Integer> DocSum;
		private TreeMap<String, Integer> WordSum;
		private int totalDocument;
		
		public void setup(Context context) throws IOException {
			Path [] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches == null || caches.length < 4) {
				System.err.print("Error");
				System.exit(2);
			}
			String buf;
			String [] word;
			
			DocSum = new TreeMap<String, Integer>();
			DocSum.clear();
			BufferedReader DocWordReader = new BufferedReader(new FileReader(caches[2].toString()));
			while ((buf = DocWordReader.readLine()) != null) {
				word = buf.split("\t");
				int value = Integer.parseInt(word[1]);
				if (word[0].charAt(0) == '-') {
					totalDocument = value;
				} else {
					DocSum.put(word[0], value);
				}
			}
			WordSum = new TreeMap<String, Integer>();
			WordSum.clear();
			BufferedReader WordSumReader = new BufferedReader(new FileReader(caches[3].toString()));
			while ((buf = WordSumReader.readLine()) != null) {
				word = buf.split("\t");
				int value = Integer.parseInt(word[1]);
				WordSum.put(word[0], value);
			}
		}
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String [] word = key.toString().split(" ");
			int t = Integer.parseInt(value.toString());
			double ret = t;
			ret /= (double) DocSum.get(word[0]);
			ret *= Math.log((double)(totalDocument) / WordSum.get(word[1])) * 1000;
			context.write(new Text(word[0]), new Text(String.format("%f %s", ret, word[1])));
		}
	}
	
	/*
	 * 将所有tf-idf求和输出
	 */
	public static class DoubleSumReducer extends Reducer<Text, Text, Text, Text> {
		//private final static int kLimitWord = 300;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			PriorityQueue<Word> Q = new PriorityQueue<Word>();
			Q.clear();
			for (Text itr:values) {
				Word word = new Word(itr.toString());
				Q.add(word);
				if (Q.size() > Document.kDimention) Q.poll();
			}
			String result = "";
			for (java.util.Iterator<Word> it = Q.iterator(); it.hasNext(); ) {
				Word word = it.next();
				result = result + String.format(" %s,%f", word.word, word.tfidf);
			}
			context.write(key, new Text(result));
		}
	}
	
	/*
	 * KMean initialization Mapper
	 */
	
	public static class InitMapper extends Mapper<Text, Text, IntWritable, Text> {
		private static IntWritable one = new IntWritable(1);
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(one, value);
		}
	}
	
	/*
	 * KMeans initialization Reducer
	 */
	public static class InitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			IntWritable OutputKey = new IntWritable();
			int K = Integer.parseInt(context.getConfiguration().get("K"));
			int id = 0;
			for (Text itr: values) {
				OutputKey.set(id);
				context.write(OutputKey, itr);
				++id;
				if (id == K) break;
			}
		}
	}
	
	/*
	 * KMeans Mapper
	 */
	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		Document [] center;
		int K;
		
		public void setup(Context context) throws IOException {
			Path [] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (caches == null) {
				System.err.print("cannnot open center file");
				System.exit(3);
			}
			K = Integer.parseInt(context.getConfiguration().get("K"));
			BufferedReader reader = new BufferedReader(new FileReader(caches[caches.length - 1].toString()));
			center = new Document[K];
			for (int i = 0; i < K; i++) {
				String str = reader.readLine();
				center[i] = new Document(str);
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Document cur = new Document(value.toString());
			double max_dist = -1;
			int tag = -1;
			for (int i = 0; i < K; i++) {
				double dist = Document.getEulerDist(center[i], cur);
				if (dist > max_dist) {
					max_dist = dist;
					tag = i;
				}
			}
			if (tag != -1) context.write(new IntWritable(tag), value);
		}
	}
	
	/*
	 * KMeans Reducer
	 */
	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<String, Double> dict = new TreeMap<String, Double>();
			int total_doc = 0;
			Document doc;
			dict.clear();
			for (Text itr : values) {
				++total_doc;
				doc = new Document(itr.toString());
				for (int i = 0; i < doc.word.length; i++) {
					Word cur = doc.word[i];
					if (dict.containsKey(cur.word)) {
						double tv = dict.get(cur.word);
						dict.remove(cur.word);
						tv += cur.tfidf;
						dict.put(cur.word, tv);
					} else {
						dict.put(cur.word, cur.tfidf);
					}
				}
			}
			PriorityQueue<Word> Q = new PriorityQueue<Word>();
			Q.clear();
			while (dict.size() > 0) {
				String cword;
				double ctf;
				cword = dict.firstKey();
				ctf = dict.get(cword) / total_doc;
				dict.remove(cword);
				Q.add(new Word(cword, ctf));
				if (Q.size() > Document.kDimention) Q.poll();
			}
			String result = "";
			for (java.util.Iterator<Word> it = Q.iterator(); it.hasNext(); ) {
				Word word = it.next();
				result = result + String.format(" %s,%f", word.word, word.tfidf);
			}
			context.write(key, new Text(result));
		}
	}
	
	/*
	 * Output Reducer
	 */
	public static class OutputReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String [] str;
			for (Text itr : values) {
				str = itr.toString().split("\\s+");
				context.write(key, new Text(str[0]));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {     
		if (args.length != 4) {
			System.err.print("usage : java -jar ***.jar [k] [maxIterations] [Input path] [Output path]");
			System.exit(2);
		}
		String local_dict = "./src/word.txt";
		String local_stopdict = "./src/stopword";
		String dst_dict = "/tmp/try/word.txt";
		String dst_stopdict = "/tmp/try/stopword.txt";
		InputStream in = new BufferedInputStream(new FileInputStream(local_dict));
		InputStream in2 = new BufferedInputStream(new FileInputStream(local_stopdict));
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(dst_dict), conf);
		OutputStream out = fs.create(new Path(dst_dict));
		IOUtils.copyBytes(in, out, 4096, true);
		fs = FileSystem.get(URI.create(dst_stopdict), conf);
		out = fs.create(new Path(dst_stopdict));
		IOUtils.copyBytes(in2, out, 4096, true);
		
		Path dictFile = new Path("/tmp/try/word.txt");
       DistributedCache.addCacheFile(dictFile.toUri(), conf);
       dictFile = new Path("/tmp/try/stopword.txt");
       DistributedCache.addCacheFile(dictFile.toUri(), conf);

		//提取网页内容并分词
		Job job = new Job(conf, "HtmlCovertion");
		job.setJarByClass(PageCluster.class);
		job.setMapperClass(getHtmlMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/try/doc_word_count"));
		
		job.waitForCompletion(true);
		
		//计算每个网页的词的总数
		job = new Job(conf, "SumDocument");
		job.setJarByClass(PageCluster.class);
		job.setMapperClass(getDocumentNameMapper.class);
		job.setReducerClass(DocumentWordSumReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tmp/try/doc_word_count"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/try/doc_word_sum"));
		                          
		job.waitForCompletion(true);
		
		Path doc_word_sum = new Path("/tmp/try/doc_word_sum/part-r-00000");
		DistributedCache.addCacheFile(doc_word_sum.toUri(), conf);
		
		//计算每个词在哪些文章中出现过
		job = new Job(conf, "SumWord");
		job.setJarByClass(PageCluster.class);
		job.setMapperClass(getWordMapper.class);
		job.setReducerClass(WordSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tmp/try/doc_word_count"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/try/word_sum"));
		                          
		job.waitForCompletion(true);
		
		Path word_sum = new Path("/tmp/try/word_sum/part-r-00000");
		DistributedCache.addCacheFile(word_sum.toUri(), conf);
		
		//计算每篇文档中每个词的tf * idf
		job = new Job(conf, "Calculate tf-idf");
		job.setJarByClass(PageCluster.class);
		job.setMapperClass(calcTFIDFMapper.class);
		job.setReducerClass(DoubleSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tmp/try/doc_word_count"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/try/word-tf-idf"));
		
		job.waitForCompletion(true);
		
		int K = Integer.parseInt(args[0]);
		int MaxIteration = Integer.parseInt(args[1]);
		conf.set("K", String.valueOf(K));

		//KMeans Initialization
		job = new Job(conf, "KMeans Init");
		job.setJarByClass(PageCluster.class);
		job.setMapperClass(InitMapper.class);
		job.setReducerClass(InitReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("/tmp/try/word-tf-idf/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/try/result"));
		
		job.waitForCompletion(true);
		
		//KMeans
		for (int i = 0; i <= MaxIteration; i++) {
			Path prevCenter = new Path("/tmp/try/result/part-r-00000");
			Path curCenter = new Path("/tmp/try/word-tf-idf/" + String.valueOf(i));
			if (fs.rename(prevCenter, curCenter) == false) System.out.println("!");
			fs.delete(new Path("/tmp/try/result"), true);
			
			curCenter = new Path("/tmp/try/word-tf-idf/" + String.valueOf(i));
			DistributedCache.addCacheFile(curCenter.toUri(), conf);
			
			if (i == MaxIteration) {
				job = new Job(conf, "Output Result");
				job.setJarByClass(PageCluster.class);
				job.setMapperClass(KMeansMapper.class);
				job.setReducerClass(OutputReducer.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				FileInputFormat.setInputPaths(job, new Path("/tmp/try/word-tf-idf/part-r-00000"));
				FileOutputFormat.setOutputPath(job, new Path(args[3]));
				
				System.exit(job.waitForCompletion(true) ? 0 : 1);
				break;
			}
			
			job = new Job(conf, "KMeans");
			job.setJarByClass(PageCluster.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, new Path("/tmp/try/word-tf-idf/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path("/tmp/try/result"));
			
			job.waitForCompletion(true);
		}
	}
	
}
