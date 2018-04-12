package secondSort.tempSecondSort_2;

import java.io.IOException;

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

public class TempSecondSort_2 {

	public static class MyMapper extends Mapper<LongWritable, Text, TempInfo, NullWritable>{

		static{
			System.out.println("begin to MyMapper...");
		}
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			TempInfo tempInfo=new TempInfo(value);
			context.write(tempInfo, NullWritable.get());
			System.out.println("value="+value.toString());
		}
		
	}
	
	public static class MyReducer extends Reducer<TempInfo, NullWritable, Text, Text>{
	    static{
	    	System.out.println("begin to MyReducer...");
	    }
		Text outKey=new Text();
		Text outValue=new Text();
		
		@Override
		protected void reduce(TempInfo key, Iterable<NullWritable> values,
				Reducer<TempInfo, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		   for(NullWritable value:values){
			   outKey.set(key.getStrDate());
			   outValue.set(key.getTemp()+"℃");
			   context.write(outKey, outValue);
		   }
			
		}				
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		final String FS_DEFAULTFS="fs.defaultFS";
		final int tasks=3;
		Path inputPath=new Path(HDFS_IP+"/TempSecondSort");
		Path outputPath=new Path(HDFS_IP+"/TempSecondSort/output");
		Path outputFile=new Path(outputPath.toString()+"/part-r-00000");
		
		Configuration conf=new Configuration();
		conf.set(FS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(TempInfo.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MySortComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);		
		job.setNumReduceTasks(tasks);
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		job.waitForCompletion(true);
		if(fs.exists(outputFile)){
			FSDataInputStream fd=fs.open(outputFile);
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
		
	}

}
