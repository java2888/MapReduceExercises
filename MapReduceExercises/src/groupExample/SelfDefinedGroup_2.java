package groupExample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SelfDefinedGroup_2 {
	final static String delimiter="\\s+";
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str=value.toString().split(delimiter);
			context.write(new Text(str[0]), new Text(str[1]));
		}

	}
    
	public static class MyGroupComparator extends WritableComparator {

		
		
		public MyGroupComparator() {
			super(Text.class,true);
			 
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text p1=(Text)a;
			Text p2=(Text)b;
			return 0;
			//return p1.compareTo(p2);
			//return super.compare(a, b);
		}
	
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count=0;
			for(Text value:values){
				context.write(key, new Text(String.valueOf(count++)));
				System.out.println("in the for--"+key.toString()+"-----"+count);
			}
			System.out.println("out of for*****"+key.toString()+"----"+count);
		}		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_DEFAULTFS="fs.defaultFS";
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Path inputPath=new Path(HDFS_IP+"/selfDefinedGroup");
		Path outPath=new Path(HDFS_IP+"/selfDefinedGroup/output");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(SelfDefinedGroup_2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 		job.setGroupingComparatorClass(MyGroupComparator.class);
 		
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
		if(fs.exists(new Path(outPath.toString()+"/part-r-00000"))){
			FSDataInputStream fd=fs.open(new Path(outPath.toString()+"/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}
		
	}

}
