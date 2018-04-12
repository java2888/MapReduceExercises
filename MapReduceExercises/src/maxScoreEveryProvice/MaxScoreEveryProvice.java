package maxScoreEveryProvice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxScoreEveryProvice {

	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		static {
			System.out.println("Begin MaxScoreEveryProvice.MyMapper...");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("Begin map");
			String line = value.toString();
			System.out.println("line=" + line);
			String[] str = line.split("\\s+");
			/*String[] str = line.split("\t");		*/	
			System.out.println(str[0] + "; " + str[1]);
			if (str.length == 2) {
				context.write(new Text(str[0]), new IntWritable(Integer.parseInt(str[1])));
				System.out.println("key=" + str[0]);
			}
		}
	}

	static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		static {
			System.out.println("Begin MyReduce...");
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (max < value.get()) {
					max = value.get();
				}
			}
			context.write(key, new IntWritable(max));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaxScoreEveryProvice.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath = new Path("hdfs://192.168.137.101:9000/grade.txt");
		Path outputPath = new Path("hdfs://192.168.137.101:9000/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
