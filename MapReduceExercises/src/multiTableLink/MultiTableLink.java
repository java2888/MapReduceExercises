package multiTableLink;

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
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiTableLink {

	private static final String fileName_factory = "factory.txt";
	private static final String fileName_address = "address.txt";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final String delimiter = "\t";

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			 
			//super.setup(context);
			System.out.println("###################"+((FileSplit) context.getInputSplit()).getPath().getName());
		}

		private String getFileName(Context context) {
			// System.out.println(((FileSplit)
			// context.getInputSplit()).getPath().getName());
			return ((FileSplit) context.getInputSplit()).getPath().getName();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter);
			String fileName = getFileName(context);
			System.out.println("fileName=" + fileName);
			if (fileName.equals(fileName_factory)) {
				String factoryName = str[0];
				String addressId = str[1];
				context.write(new Text(addressId), new Text("0," + factoryName));
				System.out.println("addressId=" + addressId + "; factoryName=" + factoryName);
			} else if (fileName.equals(fileName_address)) {
				String addressName = str[1];
				String addressId = str[0];
				context.write(new Text(addressId), new Text("1," + addressName));
				System.out.println("addressId" + addressId + "; addressName" + addressName);
			}
			System.out.println("str[0]=" + str[0] + "; str[1]=" + str[1]);
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static final String delimiter = ",";
		static {
			System.out.println("Begin MyReducer...");
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			context.write(new Text("FactoryName"), new Text("AddressName"));
			context.write(new Text("----------"), new Text("-----------"));
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Set<String> factorySet = new HashSet<String>();
			Set<String> addressSet = new HashSet<String>();

			for (Text value : values) {
				System.out.println("reduce:value=" + value.toString());
				String[] str = value.toString().split(delimiter);
				if (str[0].equals("0")) {
					factorySet.add(str[1]);
				} else if (str[0].equals("1")) {
					addressSet.add(str[1]);
					// addressName=str[1];
				}
			} // for
			for (String factoryName : factorySet) {
				for (String addressName : addressSet) {
					context.write(new Text(factoryName), new Text(addressName));
				} // for
			} // for
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		Path inputPath = new Path(HDFS_IP + "multiTableLink");
		Path outputPath = new Path(HDFS_IP + "multiTableLink/output");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(MultiTableLink.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.out.println("inputPath="+inputPath.toString());
		job.waitForCompletion(true);
		System.out.println("-----------------------------");
		if (fs.exists(new Path(outputPath.toString() + "/part-r-00000"))) {
			FSDataInputStream fd = fs.open(new Path(outputPath + "/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		} else {
			System.out.println("No result!");
		}
	}

}
