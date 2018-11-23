package lab;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;

public class WCsort {
	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		IntWritable k=new IntWritable();
		Text v=new Text();
		public void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
			String[] str=lines.toString().split("	");
			if(str.length==3){
				k.set(Integer.parseInt((str[0].trim())));
				v.set(str[1]);
				context.write(k, v);
			}
		}
	}
	public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			File f=new File("/home/zx/workspace2/wordcount.txt");
			for(Text temp:values){
				String str=temp.toString();
				FileUtils.writeStringToFile(f, str, "UTF-8", true);
				context.write(key, temp);
			}
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	   if (otherArgs.length != 2) {
	     System.err.println("Usage: invertindex <in> <out>");
	     System.exit(2);
	    }
		Job job = new Job(conf, "sort");
	   job.setJarByClass(lab.class);
	   job.setMapperClass(SortMapper.class);
	   job.setReducerClass(SortReducer.class);
	   job.setMapOutputKeyClass(IntWritable.class);
	   job.setMapOutputValueClass(Text.class);
	   job.setOutputKeyClass(IntWritable.class);
	   job.setOutputValueClass(Text.class);
	   FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	   FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	   job.setOutputFormatClass(SequenceFileOutputFormat.class);
	   if(job.waitForCompletion(true)){
		   File f=new File("/home/zx/workspace2/wordcount.txt");
		   File newf=new File("/home/zx/workspace2/wordcount_final.txt");
		   List<String> lines = FileUtils.readLines(f);
		   Collections.reverse(lines);
		   for(String line:lines){
			   FileUtils.writeStringToFile(newf, line+"\n", "UTF-8", true);
		   }
	   }
	}
}
