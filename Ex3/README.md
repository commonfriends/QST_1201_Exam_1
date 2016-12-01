题目3：编写MapReduce，统计这两个文件

`/user/hadoop/mapred_dev_double/ip_time`

`/user/hadoop/mapred_dev_double/ip_time_2`

当中，重复出现的IP的数量(40分)

---
加分项：

1. 写出程序里面考虑到的优化点，每个优化点+5分。
2. 额外使用Hive实现，+5分。
3. 额外使用HBase实现，+5分。

package lx;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class samip {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("file_path", args[2]);
		Job job = Job.getInstance(conf, "a" + samip.class);
		job.setJobName("samip");
		job.setJarByClass(samip.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private HashSet<String> hs = new HashSet<String>();

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path pp = new Path(conf.get("file_path"));
			InputStream is = fs.open(pp);
			Scanner sc = new Scanner(is);
			while (sc.hasNext()) {
				String i = sc.nextLine();
				String[] a = i.split("\t");
				hs.add(a[0]);
			}
			sc.close();
			is.close();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] a = line.split("\t");
			Iterator<String> it = hs.iterator();
			while (it.hasNext()) {
				String s = it.next();
				if (a[0].equals(s)) {
					context.write(new Text(a[0]), new Text(a[1]));
				}
			}
		}
	}
}运行jar包找出两个文件中相同的ip，对输出的文件进行去重，统计ip数
