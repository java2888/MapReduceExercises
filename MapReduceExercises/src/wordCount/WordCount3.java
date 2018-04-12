package wordCount;

import java.io.IOException;
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

public class WordCount3 {

	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word, one);
				System.out.println("word="+word.toString());
			}
		}

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputPath=new Path("hdfs://192.168.137.101:9000/README.txt");
		Path outputPath=new Path("hdfs://192.168.137.101:9000/output");
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		Job job=Job.getInstance(conf);
		job.setJarByClass(WordCount3.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem fileSystem=FileSystem.get(conf);
				if(fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		FileInputFormat.setInputPaths(job, inputPath);
		System.exit(job.waitForCompletion(true)?0:1);
				
	}

}
