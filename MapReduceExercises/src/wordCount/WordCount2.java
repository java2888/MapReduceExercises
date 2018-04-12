package wordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word=new Text();
		
		public void map(Object key, Text value,  Context context)
				throws IOException, InterruptedException {
			System.out.println("Begin map...");
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());				
				context.write(word,one);
				System.out.println(word.toString());
			}
		}

	}

	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		 
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int result = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				result += it.next().get();
			}
			context.write(key, new IntWritable(result));
			System.out.println("key=" + key.toString() + "; result=" + result);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputPath = new Path("hdfs://192.168.137.101:9000/README.txt");
		Path outputPath = new Path("hdfs://192.168.137.101:9000/output");

		System.out.println("Begin WordCount2 ...");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		//conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// job.addFileToClassPath(inputPath);
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		FileInputFormat.addInputPath(job, inputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
