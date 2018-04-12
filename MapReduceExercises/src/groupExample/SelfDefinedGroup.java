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

public class SelfDefinedGroup {
    final static String delimiter="\\s+"; //"\t";
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        
		static{
			System.out.println("Begin to MyMapper...");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 String[] str=value.toString().split(delimiter);
			 context.write(new Text(str[0]), new Text(str[1]));
			 System.out.println("value="+value.toString());
			 System.out.println("str[0]="+str[0]+"; str[1]="+str[1]);
		}
		
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		static{
			System.out.println("Begin to MyReducer...");
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count=0; 
			for(Text value:values){
				context.write(new Text("in----"+key.toString()), new Text(String.valueOf(count++)));
				System.out.println("value="+value.toString());
			}
			context.write(new Text("out***"+key.toString()), new Text("-----"));
			System.out.println("key="+key);
		}
		
		
	}
	
	public static class MyGroupComparator extends WritableComparator{



		public MyGroupComparator() {
			super(Text.class,true);
			// TODO Auto-generated constructor stub
		}


		public MyGroupComparator(Class<? extends WritableComparable> keyClass, boolean createInstances) {
			super(keyClass, createInstances);
			// TODO Auto-generated constructor stub
		}

		public MyGroupComparator(Class<? extends WritableComparable> keyClass, Configuration conf,
				boolean createInstances) {
			super(keyClass, conf, createInstances);
			// TODO Auto-generated constructor stub
		}

		public MyGroupComparator(Class<? extends WritableComparable> keyClass) {
			super(keyClass);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text p1=(Text)a;
			Text p2=(Text)b;
			//return p1.compareTo(p2);
			return 0;
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		final String HDFS_DEFAULTFS="fs.defaultFS";
		Path inputPath=new Path(HDFS_IP+"/selfDefinedGroup/SelfDefinedGroup.txt");
		Path outputPath=new Path(HDFS_IP+"/selfDefinedGroup/output");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(SelfDefinedGroup.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(MyGroupComparator.class);
		FileSystem fs=inputPath.getFileSystem(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
		if(fs.exists(new Path(outputPath.toString()+"/part-r-00000"))){
			FSDataInputStream fd=fs.open(new Path(outputPath.toString()+"/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		}else{
			System.out.println("No result.");
		}

	}

}
