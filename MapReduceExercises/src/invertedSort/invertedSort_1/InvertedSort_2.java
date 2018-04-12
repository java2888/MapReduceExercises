package invertedSort.invertedSort_1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedSort_2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		private static String fileName="";
		private final static String delimiter="\\s+";
		
		@Override		
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 fileName=((FileSplit)context.getInputSplit()).getPath().getName();
			 System.out.println("fileName="+fileName);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str=value.toString().split(delimiter);
			Text v=new Text(fileName+":1;");
			for(int i=0; i<str.length; i++){
				context.write(new Text(str[i]), v);
			}
		}
	
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		final String delimiter=":";
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 Map<String,Integer> map=new HashMap<String,Integer>();
			 for(Text value:values){
				 String[] str=value.toString().split(delimiter);
				 String strKey=str[0];
				 int num=Integer.parseInt(str[1].substring(0, str[1].length()-1));
				 int i=0;
				 if(map.containsKey(strKey)){
					 i=map.get(strKey);
					 i=i+num;
				 }else{
					 i=num;
				 }
				 map.put(strKey, i);
			 }
			 StringBuilder sb=new StringBuilder();
			 for(Map.Entry<String, Integer> entry:map.entrySet()){
				 sb.append(entry.getKey()+":"+entry.getValue()+";");
			 }
			 context.write(key, new Text(sb.toString()));		 
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/"; 
		final String HDFS_DEFAULTFS="fs.defaultFS";
		Path inputPath=new Path(HDFS_IP+"/invertedSort/input");
		Path outputPath=new Path(HDFS_IP+"/invertedSort/output");
		Path outputFile=new Path(HDFS_IP+"/invertedSort/output/part-r-00000");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(InvertedSort_2.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		if(fs.exists(outputFile)){
			FSDataInputStream fsd=fs.open(outputFile);
			IOUtils.copyBytes(fsd, System.out, 1024, true);
		}
	}

}
