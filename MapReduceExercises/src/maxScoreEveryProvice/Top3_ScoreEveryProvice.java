package maxScoreEveryProvice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
 

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top3_ScoreEveryProvice {

		
/*	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split("\\s+");
			String provice = str[0];
			
			String grade = str[1];
			//context.write(new Text(provice), new KeyPair(provice, Long.parseLong(grade)));
			context.write(new Text(provice), new LongWritable(Long.parseLong(grade)));
			System.out.println(new KeyPair(provice, Long.parseLong(grade)).toString());
		}
	}*/

/*	public static class MyReducer extends Reducer<Text, KeyPair, Text, LongWritable> {
		static{
			System.out.println("Begin MyReducer...");
		}
		@Override
		protected void reduce(Text key, Iterable<KeyPair> values,
				Context context) throws IOException, InterruptedException {
			
			//Iterator<KeyPair> itr=KeyPair.iterator();
			System.out.println("reduce");
			System.out.println(values.toString());
			 for(int i=0; (i<3) && (values.iterator().hasNext()); i++){
				 context.write(key,new LongWritable(values.iterator().next().getGrade()));
			 }
		}

	}*/

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		static{
			System.out.println("Begin MyMapper...");
		}
		@Override
		protected void map(LongWritable key, Text value,  Context context)
				throws IOException, InterruptedException {
			 String[] str=value.toString().split("\\s+");
			 String provice=str[0];
			 String grade=str[1];
			 context.write(new Text(provice), new LongWritable(Long.parseLong(grade)));
			 System.out.println(new KeyPair(provice, Long.parseLong(grade)).toString());
		}
	
	}//end of MyMapper
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			
			ArrayList<Long> arr=new ArrayList<Long>();
			for(LongWritable value:values){
				arr.add(value.get());
				System.out.println("key="+key.toString()+"; value="+value.toString());
			}
			 
			for(int i=0; i<arr.size(); i++){
				System.out.println("Before sorting:" +arr.get(i));
			}			
			Collections.sort(arr);
			 
			for(int i=0; i<arr.size(); i++){
				System.out.println("After sorting:" +arr.get(i));
			}			
			Collections.reverse(arr);
			
			StringBuilder sb=new StringBuilder();
			int i;
			Long temp = 0L;
			for( i=0; (i<3)&&(i<arr.size()); i++){
				 temp=arr.get(i);
				 sb.append(temp.toString()+" ");
				 System.out.println(temp.toString());
				  
			}
			//处理:重复相同的值,例如: 100,95,90,90,90,90
			for(int j=i-1; (j<arr.size()) &&(  arr.get(j) == arr.get(i).longValue()  ); j++){
				 sb.append(arr.get(i).toString()+" ");
				 System.out.println("Do with repeatting..."+arr.get(i).toString());
			}
			context.write(key, new Text(sb.toString()));
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		Job job=Job.getInstance(conf);
		job.setJarByClass(Top3_ScoreEveryProvice.class);
		job.setMapperClass(MyMapper.class);
		//job.setGroupingComparatorClass(GroupingComparator.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputPath=new Path(HDFS_IP+"grade.txt");
		Path outputPath=new Path(HDFS_IP+"gradeDir");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//System.exit(job.waitForCompletion(true)?0:1);
		
		job.waitForCompletion(true);
		FSDataInputStream fr=fs.open(new Path(HDFS_IP+"gradeDir/part-r-00000"));
		IOUtils.copyBytes(fr, System.out, 1024,true);
	}

 

}
