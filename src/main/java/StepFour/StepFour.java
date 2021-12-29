package StepFour;

import StepThree.StepThree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StepFour {
//    protected static int wordsInCorpus = 0;
    protected static AtomicBoolean isSetUp = new AtomicBoolean(false);
    public static class MapperClass
            extends Mapper<Text, MapWritable, Text, DoubleWritable> {

//        public void setup(Mapper.Context context) throws IOException, InterruptedException {
//            if(isSetUp.compareAndSet(false, true)){
//                super.setup(context);
//                wordsInCorpus=(context.getConfiguration().getInt("wordsInCorpus",0));
//            }
//        }

        public void map(Text key, MapWritable value, Context context
        ) throws IOException, InterruptedException {
            double n1 = ((IntWritable)value.get(new Text("w3"))).get();
            double n2 = ((IntWritable)value.get(new Text("w2w3"))).get();
            double n3 = ((IntWritable)value.get(new Text("w1w2w3"))).get();
            double cO = ((IntWritable)value.get(new Text("words"))).get();
            double c1 = ((IntWritable)value.get(new Text("w2"))).get();
            double c2 = ((IntWritable)value.get(new Text("w1w2"))).get();
            double k2 = (Math.log(n2+1)+1)/(Math.log(n2+1)+2);
            double k3 = (Math.log(n3+1)+1)/(Math.log(n3+1)+2);
            double finalProbability = k3*(n3/c2)+(1-k3)*k2*(n2/c1)+(1-k3)*(1-k2)*(n1/cO);
            context.write(key,new DoubleWritable(finalProbability));
//            IntWritable occurrences = (IntWritable) value.get(new Text("w1w2w3"));
//            wordsInCorpus = wordsInCorpus + occurrences.get();
//            wordsInCorpus.getAndAdd(occurrences.get());
//            context.write(key,value);
        }
    }

    public static class ReducerMap
            extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(DoubleWritable value: values){
                context.write(key, value);
            }
//            for(MapWritable map : values){//I HAVE ONLY ONE KEY OF 3GRAM
//                double n1 = ((IntWritable)map.get(new Text("w3"))).get();
//                double n2 = ((IntWritable)map.get(new Text("w2w3"))).get();
//                double n3 = ((IntWritable)map.get(new Text("w1w2w3"))).get();
//                double cO = wordsInCorpus;
//                double c1 = ((IntWritable)map.get(new Text("w2"))).get();
//                double c2 = ((IntWritable)map.get(new Text("w1w2"))).get();
//                double k2 = (Math.log(n2+1)+1)/(Math.log(n2+1)+2);
//                double k3 = (Math.log(n3+1)+1)/(Math.log(n3+1)+2);
//                double finalProbability = k3*(n3/c2)+(1-k3)*k2*(n2/c1)+(1-k3)*(1-k2)*(n1/cO);
//                context.write(key,new DoubleWritable(finalProbability));
//            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step four");
        job.setJarByClass(StepFour.class);
        job.setMapperClass(StepFour.MapperClass.class);
        job.setCombinerClass(StepFour.ReducerMap.class);
        job.setReducerClass(StepFour.ReducerMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
