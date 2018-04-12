package findCommonFriendsOrFans.findCommonFans_2;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

public class FindAllStarsOneFanLikes {
	final static String delimiter_1 = ":";
	final static String delimiter_2 = ",";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter_1);
			String strStar = str[0];
			String[] strFans = str[1].split(delimiter_2);
			Text NewKey = new Text();
			Text NewValue = new Text(strStar);			
			for (int i = 0; i < strFans.length; i++) {
				NewKey.set(strFans[i]);
				context.write(NewKey, NewValue);
			}
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString() + ",");
			}
			context.write(key, new Text(sb.toString().substring(0, sb.toString().length() - 1)));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		final String FS_DEFAULTFS = "df.defaultFS";
		Path inputPath = new Path(HDFS_IP + "/findCommonFans/findCommonFans.txt");
		Path outputPath = new Path(HDFS_IP + "/findCommonFans/output_1");
		Path outputFile = new Path(outputPath.toString() + "/part-r-00000");

		Configuration conf = new Configuration();
		conf.set(FS_DEFAULTFS, HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(FindAllStarsOneFanLikes.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.waitForCompletion(true);
/*		if (fs.exists(outputFile)) {
			FSDataInputStream fsd = fs.open(outputFile);
			IOUtils.copyBytes(fsd, System.out, 1024, true);		
		}*/

		Path outputCopyFromFile = new Path(HDFS_IP + "/findCommonFans/output_1/part-r-00000");	
		Path outputCopyToFile = new Path(HDFS_IP + "/findCommonFans/findCommonFans_2.txt");	
		if(fs.exists(outputCopyFromFile)){
			FSDataInputStream fsdIn=fs.open(outputCopyFromFile);
			OutputStream out=fs.create(outputCopyToFile, true);
			IOUtils.copyBytes(fsdIn, out, 1024, true);			
		}
		FindCommonFans.ExecuteMapReduce();
	}

}
