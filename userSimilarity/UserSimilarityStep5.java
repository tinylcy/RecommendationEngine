package userSimilarity;

import hdfs.HDFSDao;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class UserSimilarityStep5 {

	public static final Pattern DELIMITER = Pattern.compile("[\t:,]");

	public static class Step5MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
			String flag = fileSplit.getPath().getParent().getName();

			String[] tokens = DELIMITER.split(value.toString());
			if (flag.equals("UserSimilarityStep1")) {
				k.set("A");
				v.set(tokens[0] + ":" + (tokens.length - 1));
				output.collect(k, v);
			} else if (flag.equals("UserSimilarityStep3")) {
				k.set("B");
				v.set(value.toString());
				output.collect(k, v);
			} else {
				if (flag.equals("UserSimilarityStep4")) {
					k.set("C");
					v.set(value.toString());
					output.collect(k, v);
				}
			}

		}

	}

	public static class Step5ReduceClass extends MapReduceBase implements
			Reducer<Text, Text, Text, DoubleWritable> {

		private Text k = new Text();
		private DoubleWritable v = new DoubleWritable();

		Map<Integer, Integer> map_A = new HashMap<Integer, Integer>();
		Map<Integer, Integer> map_B = new HashMap<Integer, Integer>();
		Map<String, ArrayList<Integer>> map_C = new HashMap<String, ArrayList<Integer>>();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				Text line = values.next();
				String value = line.toString();
				String userID1_userID2 = null;
				String[] tokens = DELIMITER.split(value);
				if (key.toString().equals("A")) {
					map_A.put(Integer.parseInt(tokens[0]),
							Integer.parseInt(tokens[1]));
				} else {
					if (key.toString().equals("B")) {
						map_B.put(Integer.parseInt(tokens[0]),
								Integer.parseInt(tokens[1]));
					} else {
						if (key.toString().equals("C")) {
							userID1_userID2 = tokens[0] + ":" + tokens[1];
							for (int i = 2; i <= tokens.length - 1; i++) {
								if (!map_C.containsKey(userID1_userID2)) {
									map_C.put(userID1_userID2,
											new ArrayList<Integer>());
									map_C.get(userID1_userID2).add(
											Integer.parseInt(tokens[i]));
								} else {
									map_C.get(userID1_userID2).add(
											Integer.parseInt(tokens[i]));
								}
							}
						}
					}
				}
			}

			for (String value : map_C.keySet()) {
				String[] tokens = DELIMITER.split(value);
				int userID1 = Integer.parseInt(tokens[0]);
				int userID2 = Integer.parseInt(tokens[1]);
				int userID1Num = map_A.get(userID1);
				int userID2Num = map_A.get(userID2);
				ArrayList<Integer> commonMovieID = new ArrayList<Integer>();
				commonMovieID = map_C.get(userID1 + ":" + userID2);
				double sum = 0.0;
				for (Integer movieID : commonMovieID) {
					sum += Math.log(1 + map_B.get((int) movieID));
				}
				double degree = (1 / sum) / Math.sqrt(userID1Num * userID2Num);

				DecimalFormat df = new DecimalFormat("#.#####");
				k.set(userID1 + "," + userID2);
				v.set(Double.parseDouble(df.format(degree)));
				output.collect(k, v);

			}

		}

	}

	public static void run(Map<String, String> path) throws IOException {

		String input1 = path.get("SimilarityStep5Input_1");
		String input2 = path.get("SimilarityStep5Input_2");
		String input3 = path.get("SimilarityStep5Input_3");
		String output = path.get("SimilarityStep5Output");

		Configuration configuration = new Configuration();
		JobConf job = new JobConf(configuration, UserSimilarityStep5.class);

		HDFSDao hdfs = new HDFSDao(configuration);
		hdfs.rmr(output);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2),
				new Path(input3));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Step5MapClass.class);
		job.setReducerClass(Step5ReduceClass.class);

		job.set("mapred.textoutputformat.separator", ",");

		RunningJob runningJob = JobClient.runJob(job);
		while (!runningJob.isComplete()) {
			runningJob.waitForCompletion();
		}
	}

}
