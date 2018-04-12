package findCommonFriendsOrFans.findCommonFans_2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

import findCommonFriendsOrFans.findCommonFans_2.FindAllStarsOneFanLikes.MyMapper;
import findCommonFriendsOrFans.findCommonFans_2.FindAllStarsOneFanLikes.MyReducer;

public class FindCommonFans {
	final static String delimiter_1 = "\\s+";
	final static String delimiter_2 = ",";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split(delimiter_1);
			System.out.println("value=" + value.toString());

			String strFan = str[0];
			String[] strAllStar = str[1].split(delimiter_2);
			Set<String> set = new HashSet<String>();
			for (String str1 : strAllStar) {
				set.add(str1);
			}
			System.out.println("before:set=" + set);
			String[] strAll = new String[set.size()];
			strAll = set.toArray(strAll);
			/*
			 * String[] s1=new String[set.size()]; s1=set.toArray(s1);
			 */
			System.out.println("before:strAll=" + strAll);
			Arrays.sort(strAll);
			System.out.println("after:strAll=" + strAll);
			for (int i = 0; i < strAll.length - 1; i++) {
				String str1 = strAll[i];
				for (int j = i + 1; j < strAll.length; j++) {
					String str2 = strAll[j];
					context.write(new Text(str1 + "-" + str2), new Text(strFan));
				}
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
			context.write(key, new Text(sb.substring(0, sb.length() - 1)));
			System.out.println("reduce:value=" + sb.toString());
		}

	}

	public static void ExecuteMapReduce() throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		final String FS_DEFAULTFS = "df.defaultFS";
		Path inputPath = new Path(HDFS_IP + "/findCommonFans/findCommonFans_2.txt");
		Path outputPath = new Path(HDFS_IP + "/findCommonFans/output_2");
		Path outputFile = new Path(HDFS_IP + "/findCommonFans/output_2/part-r-00000");
		/*
		 * Path outputCopyFromFile = new Path(HDFS_IP +
		 * "/findCommonFans/output_1/part-r-00000"); Path outputCopyToFile = new
		 * Path(HDFS_IP + "/findCommonFans/findCommonFans_2.txt");
		 */

		/*
		 * Path inputPath = new Path(HDFS_IP +
		 * "/findCommonFans/findCommonFans.txt"); Path outputPath = new
		 * Path(HDFS_IP + "/findCommonFans/output_1"); Path outputFile = new
		 * Path(outputPath.toString() + "/part-r-00000");
		 */

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
		if (fs.exists(outputFile)) {
			FSDataInputStream fsd = fs.open(outputFile);
			IOUtils.copyBytes(fsd, System.out, 1024, true);
		}
	}
}
