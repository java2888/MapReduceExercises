package secondSort.tempSecondSort;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TempSecondSort {

	public static class MyMapper extends Mapper<LongWritable, Text, SelfDefinedTempInfo, NullWritable> {

		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.toString().length() > 0) {
				try {
					context.write(new SelfDefinedTempInfo(value), NullWritable.get());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static class MyReducer extends Reducer<SelfDefinedTempInfo, NullWritable, Text, IntWritable> {

		@Override
		protected void reduce(SelfDefinedTempInfo key, Iterable<NullWritable> values,
				Reducer<SelfDefinedTempInfo, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for (NullWritable value : values) {
				context.write(new Text(key.getStrDate()), new IntWritable(key.getTemp()));
				System.out.println("key="+key.toString());
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		final String HDFS_DEFAULTFS="fs.defaultFS";
		Path inputPath=new Path(HDFS_IP+"/TempSecondSort");
		Path outputPath=new Path(HDFS_IP+"/TempSecondSort/output");
		Path outputFile=new Path(outputPath.toString()+"/part-r-00000");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(TempSecondSort.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MySortComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setNumReduceTasks(3);
		job.setMapOutputKeyClass(SelfDefinedTempInfo.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}
		job.waitForCompletion(true);
		if(fs.exists(outputFile)){
			FSDataInputStream fd=fs.open(outputFile);
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
		
	}

}
