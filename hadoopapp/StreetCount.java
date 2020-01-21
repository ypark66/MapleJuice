import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
// Based on original code from Hadoop MapReduce Tutorial 
// @ https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//
public class StreetCount {

	public static class TokenizerMapper
			 extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
										) throws IOException, InterruptedException {
			String entry = value.toString();
			boolean in_dq_flag = false;
			int pre_indx = 0;
			int element_cnt = 0;
			char target;

			for (int i = 0; i < entry.length(); ++i) {
				target = entry.charAt(i);
				if (!in_dq_flag && target == ',') {
					element_cnt += 1;
					if (element_cnt == 30) {
						word.set(entry.substring(pre_indx, i));
						context.write(word, one);
						//System.out.println(entry.substring(pre_indx, i));
					}
					pre_indx = i + 1;
				}
				else if (!in_dq_flag && target == '"')
					in_dq_flag = true;
				else if (in_dq_flag && target == '"')
					in_dq_flag = false;
			}

			if (element_cnt == 30 && pre_indx < entry.length()) {
				word.set(entry.substring(pre_indx, entry.length()));
				context.write(word, one);
				//System.out.println(entry.substring(pre_indx, entry.length()));
			}
		}
	}

	public static class IntSumReducer
			 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
											 Context context
											 ) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(StreetCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
