package findCommonFriendsOrFans.findCommonFans_1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

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

public class FindFans {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        final String delimiter_1=":";
        final String delimiter_2=",";
        
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		    String s=value.toString();
		    int i=s.indexOf(delimiter_1);
		    if(i<0){
		    	return;
		    }else{
		        String strValue=s.substring(0, i);
		        String[] strKeyList=s.substring(i+1, s.length()).split(delimiter_2);
		        Set<String> treeSet=new TreeSet<String>();
		        for(int j=0; j<strKeyList.length;j++){
		        	treeSet.add(strKeyList[j]);
		        }
		        Set<String> treeSet_2=new TreeSet<String>(treeSet);
		        Iterator<String> iter=treeSet.iterator();
		       // treeSet.
		        while(iter.hasNext()){
		        	String str1=iter.next();
		        	Iterator<String> iter_2=treeSet_2.iterator();
		        	int flag=0;
		        	while(iter_2.hasNext()){
		        	    String str2=iter_2.next();
		        	    if((0==flag)&&(str1.equals(str2))){
		        	    	flag=1;
		        	    	continue;
		        	    }else if(1==flag){
		        	    	context.write(new Text(str1+"-"+str2), new Text(strValue));
		        	    }
		        		  
		        	}
		        }
		        
		    }
		}		
	}
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb=new StringBuilder();
			for(Text value:values){
				sb.append(value.toString()+",");
			}
			context.write(new Text(key+":"), new Text(sb.toString().substring(0, sb.toString().length()-1)));
		}
		
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP="hdfs://192.168.137.101:9000/";
		final String HDFS_DEFAULTFS="df.defaultFS";
		Path inputPath=new Path(HDFS_IP+"/findCommonFans");
		Path outputPath=new Path(HDFS_IP+"/findCommonFans/output");
		Path outputFile=new Path(outputPath+"/part-r-00000");
		
		Configuration conf=new Configuration();
		conf.set(HDFS_DEFAULTFS, HDFS_IP);
		Job job=Job.getInstance(conf);
		job.setJarByClass(FindFans.class);
		job.setMapperClass(MyMapper.class);
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
