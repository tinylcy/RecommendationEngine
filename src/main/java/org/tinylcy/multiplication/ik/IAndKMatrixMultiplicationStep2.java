package org.tinylcy.multiplication.ik;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tinylcy.driver.ItemBasedCFDriver;
import org.tinylcy.hdfs.HDFS;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class IAndKMatrixMultiplicationStep2 {

    public static class MultiplicationMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));
            int m = Integer.parseInt(conf.get("m"));

            String line = value.toString();
            String[] tokens = line.split(",");

            if (tokens[0].equals("A")) {
                for (int k = 0; k < m; k++) {
                    outputKey.set(tokens[1] + "," + k);
                    outputValue.set("A," + tokens[2] + "," + tokens[3]);
                    context.write(outputKey, outputValue);
                }
            } else if (tokens[0].equals("B")) {
                for (int i = 0; i < n; i++) {
                    outputKey.set(i + "," + tokens[2]);
                    outputValue.set("B," + tokens[1] + "," + tokens[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MultiplicationReducer extends
            Reducer<Text, Text, Text, FloatWritable> {

        private FloatWritable outputValue = new FloatWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

			/* 此处的两个map不能定义为类MultiplicationReducer的属性 */
            Map<Integer, Float> hashTableA = new HashMap<Integer, Float>();
            Map<Integer, Float> hashTableB = new HashMap<Integer, Float>();

            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String[] tokens = iterator.next().toString().split(",");
                if (tokens[0].equals("A")) {
                    hashTableA.put(Integer.parseInt(tokens[1]),
                            Float.parseFloat(tokens[2]));
                } else if (tokens[0].equals("B")) {
                    hashTableB.put(Integer.parseInt(tokens[1]),
                            Float.parseFloat(tokens[2]));
                }
            }

            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));

            float a_ij = 0;
            float b_jk = 0;
            float c_ik = 0.0f;

            for (int j = 0; j < n; j++) {
                a_ij = hashTableA.containsKey(j) ? hashTableA.get(j) : 0.0f;
                b_jk = hashTableB.containsKey(j) ? hashTableB.get(j) : 0.0f;
                c_ik += a_ij * b_jk;

            }
            if (c_ik != 0.0f) {
                outputValue.set(c_ik);
                context.write(key, outputValue);
            }
        }
    }

    public static void run() throws IOException, ClassNotFoundException,
            InterruptedException {

        String inputPath = ItemBasedCFDriver.path.get("step8InputPath");
        String outputPath = ItemBasedCFDriver.path.get("step8OutputPath");

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":");

        conf.set("n", String.valueOf(ItemBasedCFDriver.N));
        conf.set("m", String.valueOf(ItemBasedCFDriver.M));

        Job job = Job.getInstance(conf);

        HDFS hdfs = new HDFS(conf);
        hdfs.rmr(outputPath);

        job.setMapperClass(MultiplicationMapper.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setJarByClass(IAndKMatrixMultiplicationStep2.class);
        job.setNumReduceTasks(ItemBasedCFDriver.ReducerNumber);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }

}
