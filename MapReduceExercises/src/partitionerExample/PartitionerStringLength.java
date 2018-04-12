package partitionerExample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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

public class PartitionerStringLength {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static String delimiter = ";";

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("中文测试!");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("value=" + value.toString());//https://blog.csdn.net/zklth/article/details/11829563
			String[] str = value.toString().split(delimiter);
			context.write(new Text(str[0]), new Text(str[1]));
		}
	}

	public static class MyPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			int result = 0;
			result = key.toString().length();
			return result % numReduceTasks;
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULT = "fs.defaultFS";
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		Path inputPath = new Path(HDFS_IP + "/partitionerStringLength");
		Path outputPath = new Path(HDFS_IP + "/partitionerStringLength/output");

		Configuration conf = new Configuration();
		conf.set(HDFS_DEFAULT, HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(PartitionerStringLength.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		if (fs.exists(new Path(outputPath + "/part-r-00000"))) {
			FSDataInputStream fd = fs.open(new Path(outputPath + "/part-r-00000"));
			//BufferedReader bf=new BufferedReader(new InputStreamReader(fd));//��ֹ��������
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
	}

}
