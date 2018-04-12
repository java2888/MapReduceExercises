package joinUsingMapReduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LeftJoinUsingMR {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		private final static String EMPLOYEE_FILE_NAME = "employee.txt";
		private final static String SALARY_FILE_NAME = "salary.txt";
		private final static String delimiter = ",";

		/*
		 * private String getFileName(Context context) { return ((FileSplit)
		 * context.getInputSplit()).getPath().getName();
		 * 
		 * }
		 */
		private String getFileName(Context context) {
			// System.out.println(((FileSplit)
			// context.getInputSplit()).getPath().getName());
			System.out.println("getFileName();");
			return ((FileSplit) context.getInputSplit()).getPath().getName();
		}

		static {
			System.out.println("Begin to MyMapper...");
		}

		/*
		 * @Override protected void setup(Context context) throws IOException,
		 * InterruptedException {
		 * 
		 * //super.setup(context);
		 * System.out.println("###################"+((FileSplit)
		 * context.getInputSplit()).getPath().getName()); }
		 */
		/*
		 * @Override protected void setup(Context context) throws IOException,
		 * InterruptedException {
		 * System.out.println("FileName="+getFileName(context)); }
		 */

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * System.out.println(value.toString()); String[] str =
			 * value.toString().split(delimiter); context.write(new
			 * Text(str[0]), new Text("1," + str[1]));
			 * System.out.println("str[0]=" + str[0] + "; str[1]=" + str[1]);
			 * return;
			 */

			String FileName = getFileName(context);
			System.out.println("FileName=" + FileName);

			String[] str = value.toString().split(delimiter);

			if (FileName.equals(EMPLOYEE_FILE_NAME)) {
				context.write(new Text(str[0]), new Text("1," + str[1]));
			} else if (FileName.equals(SALARY_FILE_NAME)) {
				context.write(new Text(str[0]), new Text("2," + str[1]));
			}
			System.out.println("str[0]=" + str[0] + "; str[1]=" + str[1]);
			 
		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static String delimiter = ",";

		static {
			System.out.println("Begin to MyReducer...");
		}

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			context.write(new Text("company"), new Text("employee"+"   "+"Salary"));
			context.write(new Text("----------"), new Text("----------" + "----------"));
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Set<String> employeeHashSet = new HashSet<String>();
			Set<String> salaryHashSet = new HashSet<String>();
			for (Text value : values) {
				String[] str = value.toString().split(delimiter);
				if (str[0].equals("1")) {
					employeeHashSet.add(str[1]);
				} else if (str[0].equals("2")) {
					salaryHashSet.add(str[1]);
				}
				System.out.println("str[0]=" + str[0] + "; str[1]=" + str[1]);
			}
			Text salary = new Text();
			if (0 == salaryHashSet.size()) {
				salary.set("null");
			} else {
				salary.set(salaryHashSet.iterator().next());
			}
			for (String employ : employeeHashSet) {
				context.write(key, new Text(employ + " " + salary.toString()));
			}

		}

	}

	 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		final String HDFS_IP = "hdfs://192.168.137.101:9000/";
		Path inputPath = new Path(HDFS_IP + "leftJoin");
		Path outputPath = new Path(HDFS_IP + "leftJoin/output");
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", HDFS_IP);
		Job job = Job.getInstance(conf);
		job.setJarByClass(LeftJoinUsingMR.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.out.println("MyReducer()");
		System.out.println(inputPath.toString());
		System.out.println(outputPath.toString());

		job.waitForCompletion(true);
		if (fs.exists(new Path(outputPath + "/part-r-00000"))) {
			FSDataInputStream fd = fs.open(new Path(outputPath + "/part-r-00000"));
			IOUtils.copyBytes(fd, System.out, 1024, true);
		} else {
			System.out.println("No result");
		}

	}

}
