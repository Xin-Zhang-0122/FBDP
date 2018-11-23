package lab;

import java.util.List;
import java.io.File;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class lab {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		 List<String> stopwords_list = new ArrayList<String>();
		 List<String> wordcount=new ArrayList<String>();
		 
		 public void setup(Context context) throws IOException, InterruptedException{
			 super.setup(context);
			 System.out.println("OK1");
			 File stop_word=new File("/home/zx/下载/stopwords.txt");
			 List<String> stop_lines = FileUtils.readLines(stop_word);
			 for(String stop_line:stop_lines){
		    	  stopwords_list.add(stop_line.toString());
		     }
			 File f=new File("/home/zx/下载/fulldata.txt");
				List<String> lines = FileUtils.readLines(f);
				int tt=0;
				for(String line:lines){
					 for (Term i:HanLP.segment(line)){
						String a=i.toString().split("/")[0];
						if(a.length()>=1){
							if(a.charAt(0)>='0' & a.charAt(0)<='9' | a.charAt(0)==' ' | a.charAt(0)>='!' & a.charAt(0)<='z' | stopwords_list.contains(a)){
								tt++;
							}
							else{
								wordcount.add(a);
							}
						}
					}
				}
				System.out.println("OK2");
		 }
		 private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	System.out.println("OK3");
	    	for(String w:wordcount){
	    		word.set(w);
	    		context.write(word, one);
	    	}
	    }
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int k;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            System.out.println("ok!");
            k = context.getConfiguration().getInt("k", 3);
            if(sum>=k){
            	result.set(sum);
               context.write(key, result);
            }
            
        }
    }
	
    public static void main(String[] args)throws Exception{
    	Configuration conf = new Configuration();
		conf.setInt("k", Integer.parseInt(args[2]));
    	Path save = new Path("recode18");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <in> <out> k");
            System.exit(2);
        }
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(lab.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, save);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	System.exit(job.waitForCompletion(true) ? 0 : 1);
         
    }
}
