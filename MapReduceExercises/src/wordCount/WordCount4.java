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

public class WordCount4 {

	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{		
		private final static IntWritable one=new IntWritable(1);
		private static Text word=new Text();
				 
		static{
			System.out.println("Begin MyMapper...");
		}
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			 StringTokenizer st=new StringTokenizer(value.toString());
			 while(st.hasMoreTokens()){
				 word.set(st.nextToken());
				 context.write(word, one);
			 }
			 System.out.println("execute to map");
		}		
	}
	
	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		static{
			System.out.println("Begin MyReduce");
		}
		@Override 
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			 int sum=0;
			 for(IntWritable value:values){
				 sum+=value.get();
			 }
			 context.write(key, new IntWritable(sum));
			 System.out.println("execute to reduce");
		}
		
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		Job job=Job.getInstance(conf);
		job.setJarByClass(WordCount4.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inputPath=new Path("hdfs://192.168.137.101:9000/README.txt");
		Path outputPath=new Path("hdfs://192.168.137.101:9000/output");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);		 
	}

}
