package StepTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


public class StepTwo {
    protected static int w2w3Sum = 0;
    public static class MapperClass
            extends Mapper<Text, MapWritable, Text, MapWritable>{

        public void map(Text key, MapWritable value, Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");


            //<w1w2w3,data>
            context.write(new Text(key),value);

            //<*w2w3,data>
            String twoGramString="* "+threeGram[0]+" "+threeGram[1];

            MapWritable map2=new MapWritable();
            IntWritable occurrences = (IntWritable) value.get(new Text("w1w2w3"));
            map2.put(new Text("occurrences"),occurrences);

            context.write(new Text(twoGramString),map2);
        }
    }

    public static class ReducerMap
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        public  void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            w2w3Sum=context.getConfiguration().getInt("w2w3Sum",0);
        }

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;
            if(threeGram[0].equals("*")){
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                    sum += occurrences.get();
                }
                w2w3Sum =sum;
            }
            else{
                for(MapWritable map : values){//I HAVE ONLY ONE KEY OF 3GRAM
                    map.put(new Text("w2w3"),new IntWritable(w2w3Sum));
                    context.write(key,map);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step two");
        job.setJarByClass(StepTwo.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(StepTwo.ReducerMap.class);
        job.setReducerClass(StepTwo.ReducerMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}