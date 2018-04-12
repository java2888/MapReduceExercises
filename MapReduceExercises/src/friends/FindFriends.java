package friends;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


import org.apache.commons.lang.StringUtils;
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

public class FindFriends {
	private final static String strInput="hdfs://192.168.137.101:9000/findFriends";
	private final static String strOutputDir="hdfs://192.168.137.101:9000/findFriends/output";	
	
	public static class MyMapper extends Mapper <LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Start map...");
			String line=value.toString();			
			String[] ss=StringUtils.split(line, "\t");
			System.out.println("ss[0]=[" + ss[0] + "]; ss[1]=[" + ss[1] + "];" );	
			ss[0]=ss[0].trim();
			ss[1]=ss[1].trim();
			context.write(new Text(ss[0]), new Text(ss[1]));
			context.write(new Text(ss[1]), new Text(ss[0]));			
		}		
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			 Set<String> set=new HashSet<String>();
			 for(Text text:values){
				 set.add(text.toString());	
				// System.out.println(text.toString());
			 }
/*			 Iterator<String> _set=set.iterator();
			 while(_set.hasNext()){
				 String a=_set.next();
				 System.out.println(a);
			 }*/
			 if(set.size()>1){
				 for(Iterator i=set.iterator();i.hasNext();){
					 String qqName=(String)i.next();
					 for(Iterator j=set.iterator();j.hasNext();){
						 String otherQqName=(String)j.next();
						 if(!qqName.equals(otherQqName)){
							 context.write(new Text(qqName),new Text(otherQqName));
							 System.out.println("qqName=["+qqName +"]; otherQqName=["+otherQqName+"]");
						 }//if
					 }//forqqName
				 }//for
			 }//if
			 
		}//reduce
		
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.101:9000");

		System.out.println("Start...");
		Job job=Job.getInstance(conf);
		job.setJarByClass(FindFriends.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(job, strInput);
		final FileSystem fileSystem=FileSystem.get(conf);
		if(fileSystem.exists(new Path(strOutputDir))){
			fileSystem.delete(new Path(strOutputDir), true);
		}	    
		FileOutputFormat.setOutputPath(job, new Path(strOutputDir));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
