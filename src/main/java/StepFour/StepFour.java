package StepFour;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class StepFour {
    public static class MapperClass
            extends Mapper<Text, MapWritable, Result, DoubleWritable> {

        public void map(Text key, MapWritable value, Context context
        ) throws IOException, InterruptedException {
            //prob calc
            double n1 = ((IntWritable)value.get(new Text("w3"))).get();
            double n2 = ((IntWritable)value.get(new Text("w2w3"))).get();
            double n3 = ((IntWritable)value.get(new Text("w1w2w3"))).get();
            double cO = ((IntWritable)value.get(new Text("words"))).get();
            double c1 = ((IntWritable)value.get(new Text("w2"))).get();
            double c2 = ((IntWritable)value.get(new Text("w1w2"))).get();
            double k2 = (Math.log(n2+1)+1)/(Math.log(n2+1)+2);
            double k3 = (Math.log(n3+1)+1)/(Math.log(n3+1)+2);
            double finalProbability = k3*(n3/c2)+(1-k3)*k2*(n2/c1)+(1-k3)*(1-k2)*(n1/cO);
            context.write(new Result(key.toString(),finalProbability),new DoubleWritable(finalProbability));
        }
    }

    public static class ReducerClass
            extends Reducer<Result,DoubleWritable,Result, DoubleWritable> {

        public void reduce(Result key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(DoubleWritable value: values){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step four");
        job.setJarByClass(StepFour.class);
        job.setMapperClass(StepFour.MapperClass.class);
        if(args[1].equals("on")){
            job.setCombinerClass(StepFour.ReducerClass.class);
        }
        job.setReducerClass(StepFour.ReducerClass.class);
        job.setOutputKeyClass(Result.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
