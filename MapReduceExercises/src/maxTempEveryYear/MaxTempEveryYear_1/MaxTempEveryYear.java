package maxTempEveryYear.MaxTempEveryYear_1;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempEveryYear {

	static class TempMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Begin to execute map()...");
			String line=value.toString();
			String[] str=line.split("\t");
			System.out.println("str=["+str.length+"]");
			if(str.length==2){
				Date date;
				
				try {
					date=sdf.parse(str[0]);
					Calendar c=Calendar.getInstance();
					c.setTime(date);
					int year=c.get(1);
					String temp=str[1].substring(0, str[1].indexOf("C"));
					KeyPair kp=new KeyPair();
					kp.setYear(year); 
					kp.setTemp(Integer.parseInt(temp));
					System.out.println("kp=["+kp+"]; value=["+value+"];");
					context.write(kp, value);
					System.out.println("End to execute map()...");
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		}
		
	}//Mapper
	
	static class TempReducer extends Reducer<KeyPair, Text, KeyPair, Text> {

		@Override
		protected void reduce(KeyPair key, Iterable<Text> iter, Context context)
				throws IOException, InterruptedException {
			 System.out.println("Begin to execute reduce()...");
			 for(Text text : iter){
				 System.out.println("key=["+key+"]; text=["+text+"];");
				 context.write(key, text);
			 }
			 System.out.println("End to execute reduce()...");
		}
		
		
	}//Reducer
	
	public static void main(String[] args) throws Exception, InterruptedException {
		 Configuration conf=new Configuration();
		 conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");
		 try {
			System.out.println("Begin to execute main()...");
			Job job=Job.getInstance(conf);
			job.setJobName("MaxTempEveryYear");
			job.setJarByClass(MaxTempEveryYear.class);
			job.setMapperClass(TempMapper.class);
			job.setReducerClass(TempReducer.class);
			job.setMapOutputKeyClass(KeyPair.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setNumReduceTasks(3);
			job.setPartitionerClass(PartitionEveryYear.class);
			job.setSortComparatorClass(SortTemp.class);
			job.setGroupingComparatorClass(GroupEveryYear.class);
			
			String strInput="hdfs://192.168.137.101:9000/maxTemp/MaxTemp.txt";
			String strOutputDir="hdfs://192.168.137.101:9000/maxTemp/output";	
			FileSystem fs=FileSystem.get(conf);
			if(fs.exists(new Path(strOutputDir))){
				fs.delete(new Path(strOutputDir), true);
			}
			FileInputFormat.addInputPath(job, new Path(strInput));
			FileOutputFormat.setOutputPath(job, new Path(strOutputDir));
			System.out.println("End to execute main()...");
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 

	}

}
