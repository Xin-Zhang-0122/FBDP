package kmeans;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class kmeans {
	public static class cluster{
		int id;
		double x;
		double y;
		cluster(double x,double y){
			this.x=x;
			this.y=y;
		}
		cluster(String line){
			String[] spt=line.split(",");
			this.id=Integer.parseInt(spt[0]);
			this.x=Double.parseDouble(spt[1]);
			this.y=Double.parseDouble(spt[2]);
		}
	}
	public static class point{
		double x;
		double y;
		point(String line){
			String[] spt=line.split(",");
			this.x=Double.parseDouble(spt[0]);
			this.y=Double.parseDouble(spt[1]);
		}
		int choose_id(List<cluster> c){
			int min=1;
			double l;
			double first_l=100;
			for(cluster cluster:c){
				l=Math.sqrt(Math.pow(cluster.x-x,2)+Math.pow(cluster.y-y,2));
				if(l<first_l){
					min=cluster.id;
					first_l=l;
				}
			}
			return min;
		}
	}
	public static void initial(Configuration conf)throws IOException{
		List<cluster> c=new ArrayList<cluster>();
		List<point> points=new ArrayList<point>();
		int k=conf.getInt("k", 2);
		File f=new File("/home/zx/下载/Instance.txt");
		List<String> lines = FileUtils.readLines(f);
		for(String line:lines){
			point point=new point(line);
			points.add(point);
		}
		int count;
		for(count=1;count<=k;count++){
			point point=points.get(count);
			cluster cluster=new cluster(point.x,point.y);
			cluster.id=count;
			c.add(cluster);
		}
		String clusterpath=conf.get("clusterpath");
		File c_f=new File(clusterpath);
		for(cluster cluster: c){
            String str = cluster.id + "," + cluster.x + "," + cluster.y + "\n";
            FileUtils.writeStringToFile(c_f, str, "UTF-8", true);
        }
	}
	public static class kmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		List<cluster> c=new ArrayList<cluster>();
		 public void setup(Context context) throws IOException, InterruptedException{
			 super.setup(context);
	        String clusterpath = context.getConfiguration().get("clusterpath");
	        File fsi = new File(clusterpath);
	        List<String> lines = FileUtils.readLines(fsi, "UTF-8");
	        for(String line: lines){
	            cluster cluster = new cluster(line);
	            c.add(cluster);
	           }
	     }
		 IntWritable key=new IntWritable();
		 Text value = new Text();
		 public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
	        point point = new point(line.toString());
	        key.set(point.choose_id(c));
	        value.set(point.x + "," + point.y);
	        context.write(key, value);
	     }
	}
	public static class kmeansReduce extends Reducer<IntWritable,Text,Text, NullWritable>{
		Text value=new Text();
		public void reduce(IntWritable id, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			int count=0;
			double x=0,y=0,average_x=0,average_y=0;
			for(Text i:values){
				point point=new point(i.toString());
				x+=point.x;
				y+=point.y;
				count++;
			}
			average_x=x/count;
			average_y=y/count;
			int num=Integer.parseInt(context.getConfiguration().get("i"));
			value.set(id.get()+","+average_x+","+average_y);
			String clusterpath="clusterpath5"+num+ "/part-r-00000";
			File f=new File(clusterpath);
			String str=id.get()+","+average_x+","+average_y+"\n";
			FileUtils.writeStringToFile(f, str, "UTF-8", true);
			context.write(value, NullWritable.get());
		}
	}
	public static void mapreduce(String[] args,Configuration conf) throws Exception{
		Job kmeans = new Job(conf, "kmeans");
		kmeans.setJarByClass(kmeans.class);
		kmeans.setMapperClass(kmeansMapper.class);
		kmeans.setReducerClass(kmeansReduce.class);
		kmeans.setOutputKeyClass(IntWritable.class);
		kmeans.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(kmeans, new Path(args[0]));
      FileOutputFormat.setOutputPath(kmeans, new Path(conf.get("output")));
      kmeans.waitForCompletion(true);
	}
	public static void result(int times) throws IOException{
		List<cluster> c=new ArrayList<cluster>();
		List<point> points=new ArrayList<point>();
		File f=new File("/home/zx/下载/Instance.txt");
		List<String> lines = FileUtils.readLines(f);
		for(String line:lines){
			point point=new point(line);
			points.add(point);
		}
		times-=1;
		String clusterpath = "clusterpath5" + times+ "/part-r-00000";
		f=new File(clusterpath);
		lines = FileUtils.readLines(f, "UTF-8");
      for(String line: lines){
          cluster cluster = new cluster(line);
          c.add(cluster);
        }
      f=new File("/home/zx/output/result.txt");
      for(point point: points){
          int min = point.choose_id(c);
          FileUtils.writeStringToFile(f,min+","+point.x+","+point.y+"\n",true);
        }
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		String clusterpath="/home/zx/cluster/";
		String output="/home/zx/output/";
		conf.setInt("k", Integer.parseInt(args[2]));
		conf.set("clusterpath", "clusterpath50/part-r-00000");
		int times=6;
		initial(conf);
		for(int i=0;i<times;i++){
			int nexti = i+1;
            conf.set("clusterpath","clusterpath5"+i+"/part-r-00000");
            conf.set("output","clusterpath5"+nexti+"/part-r-00000");
            conf.setInt("i", nexti);
            mapreduce(args, conf);
		}
		result(times);
	}
}
