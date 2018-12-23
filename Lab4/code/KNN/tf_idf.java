package knn;
import java.util.List;
import java.io.File;
import java.lang.Math;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import java.util.ArrayList;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
public class tf_idf {
	private static ArrayList<Integer> freq = new ArrayList<Integer>();
	private static ArrayList<String> word = new ArrayList<String>();
	private static int sum;
	public static class Tf_IdfMapper extends Mapper<Object, Text, Text, Text> {
		Text Key=new Text();
		Text Value=new Text();
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			ArrayList<Double> tf_idf = new ArrayList<Double>();
			double zero=0;
			int count=0;
			for (int i=0;i<freq.size();i++){
				tf_idf.add(zero);
			}
			int total=0;
			for (Term i:HanLP.segment(line.toString().replaceAll("[a-zA-Z0-9]", ""))){
				total++;
			}
			for (Term i:HanLP.segment(line.toString().replaceAll("[a-zA-Z0-9]", ""))){
				count=0;
				String a=i.toString().split("/")[0];
				if(word.contains(a)){
					
					for (Term j:HanLP.segment(line.toString().replaceAll("[a-zA-Z0-9]", ""))){
						String b=j.toString().split("/")[0];
						if(a.equals(b)){
							count++;
							
						}
					}
					double tf=(double)count/total;
					double idf=Math.log(sum/(freq.get(word.indexOf(a))+1));
					double tfidf=tf*idf*30;
					tf_idf.set(word.indexOf(a.toString()), tfidf);
				}
			}
			
			String ti="";
			for(double i:tf_idf){
				ti+=Double.toString(i)+" ";
			}
		//	System.out.println(ti);
			Key.set(line);
			Value.set(ti);
			context.write(Key, Value);
		}
	}
	public static class Tf_IdfReducer extends Reducer<Text, Text, Text, Text> {
		Text Key=new Text();
		Text Value=new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			File f=new File("/home/zx/pos_tfidf.txt");
			String i="";
			for(Text temp:values){
				i+=temp.toString();
			}
			String str=i+"\n";
			FileUtils.writeStringToFile(f, str, "UTF-8", true);
			Key.set(key.toString());
			Value.set(i);
			context.write(Key, Value);
		}
	}
	public static void main(String[] args) throws Exception{
		sum=0;
		File f=new File("/home/zx/pos_words.txt");
		List<String> lines = FileUtils.readLines(f);
		for(String line:lines){
			String[] spt=line.toString().split(" ");
			freq.add(Integer.parseInt(spt[spt.length-1]));
			sum+=Integer.parseInt(spt[spt.length-1]);
			word.add(spt[0]);
		}
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: invertindex <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "tf_idf");
		job.setJarByClass(tf_idf.class);
		job.setMapperClass(Tf_IdfMapper.class);
		job.setReducerClass(Tf_IdfReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
