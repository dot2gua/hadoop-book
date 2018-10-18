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
 * 统计男生和女生的总人数
 */
public class Job1Class {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(Job1Class.class);
        job.setJobName("Job1 MapReduce");
        FileInputFormat.addInputPath(job, new Path("/data/zzti/lec/Job1/input"));
        FileOutputFormat.setOutputPath(job, new Path("/data/zzti/lec/Job1/output"));
        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        };

        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            context.write(new Text(toks[2]), new Text(toks[1]));
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    private static class Job1Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new Text(count + ""));
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

}
