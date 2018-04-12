/*package processMobileWebLog;

public class SelfDefinedSortTypeProcessWebLog {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}*/

package sortExample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelfDefinedSortTypeProcessWebLog {
	// upPackNum��downPackNum��upPayLoad downPayLoad

	public static class MyMapper extends Mapper<LongWritable, Text, UserAllPayLoad, NullWritable> {
		private final static String delimiter ="\\s+"; //"\t"; //"\\s+";

		static{
			System.out.println("begin to MyMapper...");
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter);
			System.out.println("value="+value.toString());
			//String mobileNumber = str[0];
			UserAllPayLoad userAllPayLoad = new UserAllPayLoad(str[0],Integer.parseInt(str[1]), Integer.parseInt(str[2]),
					Integer.parseInt(str[3]), Integer.parseInt(str[4]));
			context.write(userAllPayLoad,  NullWritable.get());
			System.out.println(userAllPayLoad);
		}

	}

	public static class MyReducer extends Reducer<UserAllPayLoad, NullWritable, UserAllPayLoad, NullWritable> {

		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			   
		       System.out.println("mobileNumber"+" "+"upPackNum"+" "+"downPackNum"+" "+"upPayLoad"+" "+"downPayLoad" 
		    		   +"allPayLoad");
		       System.out.println("--------------------------------------------------");
		}

		@Override
		protected void reduce(UserAllPayLoad key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
/*			UserAllPayLoad userAllPayLoad=new UserAllPayLoad(0,0,0,0);
			for (UserAllPayLoad value : values) {
					 userAllPayLoad.setUpPackNum(userAllPayLoad.getUpPackNum()+value.getUpPackNum());
					 userAllPayLoad.setDownPackNum(userAllPayLoad.getDownPackNum()+value.getDownPackNum());
					 userAllPayLoad.setUpPayLoad(userAllPayLoad.getUpPayLoad()+value.getUpPayLoad());
					 userAllPayLoad.setDownPayLoad(userAllPayLoad.getDownPayLoad()+value.getDownPayLoad());
			}*/
			context.write(key, NullWritable.get());
			
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULT="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Path inputPath=new Path(HDFS_IP+"/processMobileWebLog/input/orderedLog.txt");
		Path outPath=new Path(HDFS_IP+"/processMobileWebLog/output");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULT, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(SelfDefinedSortTypeProcessWebLog.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(UserAllPayLoad.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(UserAllPayLoad.class);
		job.setOutputValueClass(NullWritable.class);
		job.setCombinerClass(MyReducer.class);
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

