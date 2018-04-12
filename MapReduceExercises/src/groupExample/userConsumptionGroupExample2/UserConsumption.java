package groupExample.userConsumptionGroupExample2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
			Long cost=Long.parseLong(str[2]);
			context.write(new ConsumptionData(name, cost), NullWritable.get());
			System.out.println("value="+value.toString());
		}
		
	}
	
	
	public static class MyReducer extends Reducer<ConsumptionData, NullWritable, Text, Text>{

		static{
			System.out.println("begin to MyReducer...");
		}
		@Override
		protected void reduce(ConsumptionData key, Iterable<NullWritable> values,
				Reducer<ConsumptionData, NullWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb=new StringBuilder();
			for(NullWritable value:values){
				String strCost=key.getCost().toString();
				sb.append(strCost+",");
			}
			context.write(new Text(key.getName()), new Text(sb.toString().substring(0, sb.length()-1)));
			System.out.println("key="+key.getName()+"; sb="+sb.toString());
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULTFS="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/"; 
		Path inputPath=new Path(HDFS_IP+"/userConsumption");
		Path outputPath=new Path(HDFS_IP+"/userConsumption/output");
		Path outputFile=new Path(outputPath.toString()+"/part-r-00000");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(UserConsumption.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(ConsumptionData.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setSortComparatorClass(MySortComparator.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
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
			FSDataInputStream fd=fs.open(outputFile);
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
		

	}

}
