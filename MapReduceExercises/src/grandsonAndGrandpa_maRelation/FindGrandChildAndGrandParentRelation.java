package grandsonAndGrandpa_maRelation;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindGrandChildAndGrandParentRelation {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,  Context context)
				throws IOException, InterruptedException {
			 String[] str=value.toString().split("\\s+");
			 String son=str[0];
			 String parent=str[1];
			 context.write(new Text(son), new Text("1"+" " +parent));
			 context.write(new Text(parent), new Text("0"+" "+son));
			 System.out.println("son="+son+"; parent="+parent);
		}	
	}
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{

		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			 context.write(new Text("grandChild"), new Text("grandParent"));
			 context.write(new Text("----------"), new Text("-----------"));			 
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 Set<String> sonSet=new HashSet<String>();
			 Set<String> grandpaSet=new HashSet<String>();
			 for(Text value:values){
				 String[] str=value.toString().split("\\s+");
				 int flag=Integer.parseInt(str[0]);
				 String people=str[1];
				 if(0==flag){
					 sonSet.add(people);
				 }else{
					 grandpaSet.add(people);
				 }
			 }//for
			 for(String grandson:sonSet){
				 for(String grandpa:grandpaSet){
					 context.write(new Text(grandson), new Text(grandpa));
				 }
			 }
		}
		
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(FindGrandChildAndGrandParentRelation.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputPath=new Path(HDFS_IP+"find_Grandson_Relation/Find_Grandson_Relation.txt");
		Path outputPath=new Path(HDFS_IP+"find_Grandson_Relation/output");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		System.out.println("-----------------------------");
		if(fs.exists(new Path(HDFS_IP+"find_Grandson_Relation/output/part-r-00000"))){
			FSDataInputStream fd=fs.open(new Path(HDFS_IP+"find_Grandson_Relation/output/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);			
		}else{
			System.out.println("No result");
		}

	}

}
