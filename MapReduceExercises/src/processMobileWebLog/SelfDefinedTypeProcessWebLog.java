package processMobileWebLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class SelfDefinedTypeProcessWebLog {
	// upPackNum��downPackNum��upPayLoad downPayLoad

	public static class MyMapper extends Mapper<LongWritable, Text, Text, UserWebPayLoad> {
		private final static String delimiter = "\t"; //"\\s+";

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter);
			System.out.println("str[5]="+str[5]);
			String mobileNumber = str[1];
			UserWebPayLoad userWebPayLoad = new UserWebPayLoad(Integer.parseInt(str[6]), Integer.parseInt(str[7]),
					Integer.parseInt(str[8]), Integer.parseInt(str[9]));
			context.write(new Text(mobileNumber), userWebPayLoad);
			System.out.println("mobileNumber="+mobileNumber+": userWebPayLoad="+userWebPayLoad);
		}

	}

	public static class MyReducer extends Reducer<Text, UserWebPayLoad, Text, UserWebPayLoad> {

		
		@Override
		protected void setup(Reducer<Text, UserWebPayLoad, Text, UserWebPayLoad>.Context context)
				throws IOException, InterruptedException {
			   
		       System.out.println("mobileNumber"+" "+"upPackNum"+" "+"downPackNum"+" "+"upPayLoad"+" "+"downPayLoad");
		       System.out.println("-----------------------------------------------");
		}

		@Override
		protected void reduce(Text key, Iterable<UserWebPayLoad> values,
				Reducer<Text, UserWebPayLoad, Text, UserWebPayLoad>.Context context)
				throws IOException, InterruptedException {
			UserWebPayLoad userAllPayLoad=new UserWebPayLoad(0,0,0,0);
			for (UserWebPayLoad value : values) {
					 userAllPayLoad.setUpPackNum(userAllPayLoad.getUpPackNum()+value.getUpPackNum());
					 userAllPayLoad.setDownPackNum(userAllPayLoad.getDownPackNum()+value.getDownPackNum());
					 userAllPayLoad.setUpPayLoad(userAllPayLoad.getUpPayLoad()+value.getUpPayLoad());
					 userAllPayLoad.setDownPayLoad(userAllPayLoad.getDownPayLoad()+value.getDownPayLoad());
			}
			context.write(key, userAllPayLoad);
			
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULT="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Path inputPath=new Path(HDFS_IP+"/processMobileWebLog");
		Path outPath=new Path(HDFS_IP+"/processMobileWebLog/output");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULT, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(SelfDefinedTypeProcessWebLog.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserWebPayLoad.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserWebPayLoad.class);
		job.setCombinerClass(MyReducer.class);
		//job.setPartitionerClass(WebLogPartitioner.class);
		job.setNumReduceTasks(1);
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		if(fs.exists(new Path(outPath.toString()+"/part-r-00000"))){
			FSDataInputStream fd=fs.open(new Path(outPath.toString()+"/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
	}

}
