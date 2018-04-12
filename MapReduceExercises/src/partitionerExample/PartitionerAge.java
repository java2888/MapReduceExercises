package partitionerExample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerAge {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		final static String delimiter = "\t";
		static Text name = new Text();
		static Text age = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter);
			name.set(str[0]);
			age.set(str[1]);
			context.write(name, age);
		}

	}

	public static class MyPartitioner extends Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			int age=Integer.parseInt(value.toString());
			int result=0;
			if(age<=20){
				result=0;
			}else if(age<=50){
				result=1;
			}else if(age > 50){
				result=2;
			}
			return result % numReduceTasks;
		}
		
		
	}
	

	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		private static int total=0;
		private  int count=0;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
				total++;
				System.out.println("setup:total="+total+"; count="+count);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("reduce begin:key="+key+"; count="+count);
			for(Text value:values){		
				count++;
				context.write(key, value);
			}
			System.out.println("reduce end:key="+key+"; count="+count);	
			System.out.println("------------");
		}
				
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 System.out.println("cleanup:total="+total+"; count="+count);
			 System.out.println("------------------------");
		}
 				
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String FS_DEFAULT="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Configuration conf=new Configuration();
		conf.set(FS_DEFAULT, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(PartitionerAge.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputPath=new Path(HDFS_IP+"partitioner_age");
		Path outPath=new Path(HDFS_IP+"partitioner_age/output");
		FileSystem fs=outPath.getFileSystem(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);			
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		if(fs.exists(new Path(outPath.toString()+"/part-r-00000"))){
			FSDataInputStream fd=fs.open(new Path(outPath.toString()+"/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}else{
			System.out.println("No result.");
		}
	 
	}

}
