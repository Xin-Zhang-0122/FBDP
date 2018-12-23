package knn;
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
public class knn {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		 List<String> chi_word=new ArrayList<String>();
		 List<String> negword=new ArrayList<String>();
		 public void setup(Context context) throws IOException, InterruptedException{
			 super.setup(context);
			 File f=new File("/home/zx/下载/chi_word.txt");
			 List<String> linesc = FileUtils.readLines(f);
			 int tt=0;
			 for(String line:linesc){
				 chi_word.add(line);
			 }
			 File neg=new File("/home/zx/下载/fulldata.txt");
			 List<String> lines = FileUtils.readLines(neg);
				for(String line:lines){
					 for (Term i:HanLP.segment(line)){
						String a=i.toString().split("/")[0];
						if(a.length()>=1){
							if(a.charAt(0)>='0' & a.charAt(0)<='9' | a.charAt(0)==' ' | a.charAt(0)>='!' & a.charAt(0)<='z' ){
								tt++;
							}
							else{
								if(chi_word.contains(a)){
									negword.add(a);
								}
							}
						}
					}
				}
		 }
		 private final static IntWritable zero = new IntWritable(0);
		 private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	for(String w:chi_word){
	    		word.set(w);
	    		context.write(word, zero);
	    	}
	    	for(String w:negword){
	    		word.set(w);
	    		context.write(word, one);
	    	}
	    }
	}
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
       private IntWritable result = new IntWritable();
       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
        result.set(sum);
        File f=new File("/home/zx/train.txt");
        String str=key.toString()+"  "+sum+"\n";
        FileUtils.writeStringToFile(f, str, "UTF-8", true);
        context.write(key, result);
       }
   }
   public static void main(String[] args)throws Exception{
   	Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 2) {
           System.err.println("Usage: wordcount <in> <out> k");
           System.exit(2);
         }
       Job job = new Job(conf, "wordcount");
       job.setJarByClass(knn.class);
       job.setMapperClass(TokenizerMapper.class);
       job.setReducerClass(IntSumReducer.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       job.setOutputFormatClass(SequenceFileOutputFormat.class);
       System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}