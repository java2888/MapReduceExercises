package secondSort.tempSort;

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

public class DateWhenTempIsMax {

	public static class MyMapper extends Mapper<LongWritable, Text, TempData, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			TempData tempData = new TempData(value);
			context.write(tempData, NullWritable.get());
		}

	}

	public static class MyReducer extends Reducer<TempData, NullWritable, Text, Text> {

		@Override
		protected void reduce(TempData key, Iterable<NullWritable> values,
				Reducer<TempData, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			for (NullWritable value : values) {
				context.write(new Text(key.getStrDate()), new Text(key.getTemp() + "℃"));
				break;
			}
		}

	}

	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		final String HDFS_DEFAULTFS = "fs.defaultFS";
		final int tasks=3;
		Path inputPath = new Path(HDFS_IP + "/TempSecondSort");
		Path outputPath = new Path(HDFS_IP + "/TempSecondSort/output");
		Path outputFile = new Path(outputPath.toString() + "/part-r-00000");

		Configuration conf = new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(DateWhenTempIsMax.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setSortComparatorClass(MySortComparator.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(tasks);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setMapOutputKeyClass(TempData.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);

		if (fs.exists(outputFile)) {
			FSDataInputStream fsd = fs.open(outputFile);
			IOUtils.copyBytes(fsd, System.out, 1024, true);
		}
	}

}
