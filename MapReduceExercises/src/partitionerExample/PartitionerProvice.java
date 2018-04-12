package partitionerExample;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerProvice {
	private static String[] strProvice = { "湖南", "湖北", "北京", "上海" };
	private static Map<String, Integer> map;

	private static int getPartitionNum(String provice) {
		return map.get(provice);

	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			map = new HashMap<String, Integer>();
			for (int i = 0; i < strProvice.length; i++) {
				map.put(strProvice[i], i);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String provice = value.toString();
			context.write(new Text(provice), new IntWritable(1));
		}

	}

	public static class MyPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			if(map.containsKey(key.toString())){
				int result = map.get(key.toString());
				System.out.println("result="+result);
				return result % numReduceTasks;				
			}else{
				return 0;
			}

		}

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int result = 0;
			for (IntWritable value : values) {
				result += value.get();
			}
			context.write(key, new IntWritable(result));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		final String HDFS_DEFAULT = "fs.defaultFS";
		Path inputPath = new Path(HDFS_IP + "/PartitionerProvice");
		Path outputPath = new Path(HDFS_IP + "/PartitionerProvice/output");

		Configuration conf = new Configuration();
		conf.set(HDFS_DEFAULT, HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(PartitionerProvice.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		if (fs.exists(new Path(outputPath.toString() + "/part-r-00000"))) {
			FSDataInputStream fd = fs.open(new Path(outputPath.toString() + "/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}else{
			System.out.println("No Result!");
		}

	}

}
