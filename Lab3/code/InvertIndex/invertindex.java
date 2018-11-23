package lab;
import java.util.List;
import java.io.File;
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
public class invertindex {
	public static class InvertIndexMapper extends Mapper<Object, Text, Text, Text> {
		List<String> stopwords_list = new ArrayList<String>();
		
		public void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			File stop_word=new File("/home/zx/下载/stopwords.txt");
			List<String> stop_lines = FileUtils.readLines(stop_word);
			for(String stop_line:stop_lines){
				stopwords_list.add(stop_line.toString());
		    }
		}
		Text num_url=new Text();
		private Text word = new Text();
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			//System.out.println(line+"%%%%%%%%%%%%%%%%%%%%%%");
			String[] spt=line.toString().split("\\t");
			List<String> invertindex=new ArrayList<String>();
			int tt=0;
			if(spt.length>=3){
			for (Term i:HanLP.segment(spt[spt.length-2].replaceAll("[a-zA-Z0-9]", ""))){
				String a=i.toString().split("/")[0];
				if(a.length()>=2){
					if(a.charAt(0)>='0' & a.charAt(0)<='9' | a.charAt(0)==' ' | a.charAt(0)>='!' & a.charAt(0)<='z' | stopwords_list.contains(a)){
						tt++;
					}
					else{
						invertindex.add(a);
					}
				}
			}
			
			String stock=spt[0];
			String url=spt[spt.length-1];
			for(String str:invertindex){
				word.set(str);//stock name
				num_url.set("1,"+stock+","+url);//1+stock code+stock url
				//System.out.println(word.toString()+"   "+num_url.toString());
				context.write(word, num_url);
			}
			}
		}
	}
	public static class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {
		Text Key=new Text();
		Text Value=new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum=0;
			String stock_code="";
			String url="";
			//System.out.println(key.toString());
			for(Text temp:values){
				//System.out.println(temp.toString());
				String[] str=temp.toString().split(",");
				sum+=Integer.parseInt(str[0]);
				stock_code=stock_code+"    "+str[1];
				url=url+"    "+str[str.length-1];
			}
			Key.set(key.toString());
			//System.out.println(key.toString()+"   "+sum+"   "+stock_code+"   "+url);
			File f=new File("/home/zx/workspace2/invertindex.txt");
		   String str = key.toString()+"   "+sum+"   "+stock_code+"   "+url+ "\n";
		   FileUtils.writeStringToFile(f, str, "UTF-8", true);
			context.write(Key, new Text(sum+","+stock_code+","+url));
			
		}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if (otherArgs.length != 2) {
    	  System.err.println("Usage: invertindex <in> <out>");
        System.exit(2);
        }
      Job job = new Job(conf, "invertindex");
      job.setJarByClass(lab.class);
      job.setMapperClass(InvertIndexMapper.class);
      job.setReducerClass(InvertIndexReducer.class);
      job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}