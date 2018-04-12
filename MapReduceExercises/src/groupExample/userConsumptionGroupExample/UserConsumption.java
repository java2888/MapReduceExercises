package groupExample.userConsumptionGroupExample;

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

public class UserConsumption {
  
	final static String delimiter=",";
	public static class MyMapper extends Mapper<LongWritable, Text, ConsumptionData, NullWritable>{

		static{
			System.out.println("begin to MyMapper...");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 String[] str=value.toString().split(delimiter);
			 String name=str[1];
			 String cost=str[2];
			 context.write(new ConsumptionData(name, Integer.parseInt(cost)), NullWritable.get());
			 System.out.println("value="+value.toString());
		}
				
	}
	
	 
	public static class MyReducer extends Reducer<ConsumptionData, NullWritable, Text, Text>{

		static {
			System.out.println("begin to MyReducer...");
		}
		@Override
		protected void reduce(ConsumptionData key, Iterable<NullWritable> values,
				Reducer<ConsumptionData, NullWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 StringBuilder sb=new StringBuilder();
			 Text newKey=new Text();
			 String name=key.getName();
			 newKey.set(name);
			 for(NullWritable value:values){			 
				 Integer cost=key.getCost();
				 sb.append(String.valueOf(cost)+",");
/*				 String[] str=key.toString().split(delimiter);	 
				 newKey.set(str[0]);
				 sb.append(str[1]+",");
				 System.out.println("key="+key.toString());*/
			 }
			 context.write(newKey, new Text(sb.toString().substring(0, sb.toString().length()-1)));
			// System.out.println("key="+key.toString());
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULTFS="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Path inputPath=new Path(HDFS_IP+"/userConsumption");
		Path outputPath=new Path(HDFS_IP+"/userConsumption/output");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(UserConsumption.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		job.setMapOutputKeyClass(ConsumptionData.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		
		Path outputFile=new Path(outputPath.toString()+"/part-r-00000");
		if(fs.exists(outputFile)){
			FSDataInputStream fd=fs.open(outputFile);
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}else{
			System.out.println("No result!");
		}
	}

}
