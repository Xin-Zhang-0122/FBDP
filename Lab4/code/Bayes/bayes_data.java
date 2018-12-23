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
public class bayes_data {
	public static class Tf_IdfMapper extends Mapper<Object, Text, Text, Text> {
		Text Key=new Text();
		Text Value=new Text();
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			String[] spt=line.toString().split("  ");
			int a=Integer.parseInt(spt[1]);
			int b=Integer.parseInt(spt[2]);
			int c=Integer.parseInt(spt[3]);
			int total=a+b+c;
			if(total==0){
				total++;
			}
			double neg=(double)a/total;
			double neu=(double)b/total;
			double pos=(double)c/total;
			String ti="";
			ti=Double.toString(neg)+"  "+Double.toString(neu)+"  "+Double.toString(pos);
			Key.set(spt[0]);
			Value.set(ti);
			context.write(Key, Value);
		}
	}
	public static class Tf_IdfReducer extends Reducer<Text, Text, Text, Text> {
		Text Key=new Text();
		Text Value=new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			File f=new File("/home/zx/bayes_data.txt");
			String i="";
			for(Text temp:values){
				i+=temp.toString();
				break;
			}
			String str=i+"\n";
			FileUtils.writeStringToFile(f, str, "UTF-8", true);
			Key.set(key.toString());
			Value.set(i);
			context.write(Key, Value);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: invertindex <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "bayes_data");
		job.setJarByClass(bayes_data.class);
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
