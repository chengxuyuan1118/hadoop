package com.liwei.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class SalaryCount {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String columns[] = data.split(",");
            int depNo = Integer.parseInt(columns[7]);
            int salary = Integer.parseInt(columns[5]);
            context.write(new IntWritable(depNo),new IntWritable(salary));
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "word count");
        job.setJarByClass(com.liwei.hadoop.SalaryCount.class);
        job.setMapperClass(com.liwei.hadoop.SalaryCount.TokenizerMapper.class);
        //当map生成的数据过大时，怎么样精简压缩传给Reduce数据，可以使用它。和setReducerClass效果一样
        job.setCombinerClass(com.liwei.hadoop.SalaryCount.IntSumReducer.class);

        //job.setReducerClass(com.liwei.hadoop.SalaryCount.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/salary_count_input"));
        FileOutputFormat.setOutputPath(job, new Path("/salary_count_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
