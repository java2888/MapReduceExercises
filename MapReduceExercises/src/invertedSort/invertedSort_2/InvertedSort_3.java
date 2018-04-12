package invertedSort.invertedSort_2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedSort_3 {
	final static String delimiter_1 = "\\s+";
	final static String delimiter_2 = ":";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		static{
			System.out.println("begin to MyMapper...");
		}
		static String fileName;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			System.out.println("fileName=" + fileName);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("value=" + value.toString());
			String[] str = value.toString().split(delimiter_1);
			for (int i = 0; i < str.length; i++) {
				context.write(new Text(str[i] + delimiter_2 + fileName), new Text("1"));
				System.out.println("map:key=" + new Text(str[i] + delimiter_2 + fileName).toString());
			}

		}
	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
		static {
			System.out.println("begin to MyCombiner...");
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				 Context context) throws IOException, InterruptedException {
			System.out.println("MyCombiner:key=" + key.toString()+"; values="+values.toString());
			int result = 0;
			for (Text value : values) {
				result  =result+ Integer.parseInt(value.toString());
				System.out.println("MyCombiner:value="+value.toString()+"; result="+result);
			}
			String[] str = key.toString().split(delimiter_2);
			System.out.println("str[0]="+str[0]+"; new Text(str[1] + delimiter_2 + String.valueOf(result))="+new Text(str[1] + delimiter_2 + String.valueOf(result)).toString());
			context.write(new Text(str[0]), new Text(str[1] + delimiter_2 + String.valueOf(result)));
			
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		static {
			System.out.println("begin to MyReducer...");
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("MyReducer:key="+key.toString());
			Map<String, IntWritable> map =  new TreeMap<String, IntWritable>();//new HashMap<String, IntWritable>();
			for (Text value : values) {
				String strKey = value.toString().split(delimiter_2)[0];
				String strValue = value.toString().split(delimiter_2)[1];
				if (map.containsKey(strKey)) {
					int newValue = map.get(strKey).get() + Integer.parseInt(strValue);
					map.put(strKey, new IntWritable(newValue));
				} else {
					map.put(strKey, new IntWritable(Integer.parseInt(strValue)));
				}
			}
			 
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String, IntWritable> entry : map.entrySet()) {
				sb.append(entry.getKey() + delimiter_2 + entry.getValue() + ";");
			}
			context.write(key, new Text(sb.toString()));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		final String FS_DEFAULTFS = "fs.defaultFS";
		Path inputPath = new Path(HDFS_IP + "/invertedSort/input");
		Path outputPath = new Path(HDFS_IP + "/invertedSort/output");
		Path outputFile = new Path(HDFS_IP + "/invertedSort/output/part-r-00000");

		Configuration conf = new Configuration();
		conf.set(FS_DEFAULTFS, HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(InvertedSort_3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyCombiner.class);
/*		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
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
