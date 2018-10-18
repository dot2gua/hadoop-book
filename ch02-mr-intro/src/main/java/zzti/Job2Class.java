package zzti;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * （最大值最小值）求出生年月日最大值
 */
public class Job2Class {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(Job2Class.class);
        job.setJobName("Job2 MapReduce");
        FileInputFormat.addInputPath(job, new Path("/data/zzti/lec/Job2/input"));
        FileOutputFormat.setOutputPath(job, new Path("/data/zzti/lec/Job2/output"));
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        };

        private int maxValue = 0;
        private String maxValuePerson = "";
        private int minValue = Integer.MAX_VALUE;
        private String minValuePerson = "";

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            String[] date = toks[3].split("-");
            if (date.length == 3) {
                int termValue = Integer.parseInt(date[0] + date[1] + date[2]);
                if (maxValue < termValue) {
                    maxValue = termValue;
                    maxValuePerson = toks[1];
                }
                if (minValue > termValue) {
                    minValue = termValue;
                    minValuePerson = toks[1];
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            context.write(new Text(maxValuePerson + ""), new Text(maxValuePerson));
            context.write(new Text(minValuePerson + ""), new Text(minValuePerson));
        }
    }

    private static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        }

        private int maxValue = 0;
        private String maxValuePerson = "";
        private int minValue = Integer.MAX_VALUE;
        private String minValuePerson = "";

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            String[] date = key.toString().split("-");
            String personName = values.iterator().next().toString();
            if (date.length == 3) {
                int termValue = Integer.parseInt(date[0] + date[1] + date[2]);
                if (maxValue < termValue) {
                    maxValue = termValue;
                    maxValuePerson = personName;
                }
                if (minValue > termValue) {
                    minValue = termValue;
                    minValuePerson = personName;
                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
            context.write(new Text(maxValuePerson + ""), new Text(maxValuePerson));
            context.write(new Text(minValuePerson + ""), new Text(minValuePerson));
        }
    }

}
