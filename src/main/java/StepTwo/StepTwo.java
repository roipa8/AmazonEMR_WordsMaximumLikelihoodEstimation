package StepTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


public class StepTwo {
    protected static int w2w3Sum = 0;
    protected static int w2Sum = 0;
    protected static AtomicBoolean isSetUp = new AtomicBoolean(false);
    public static class MapperClass
            extends Mapper<Text, MapWritable, Text, MapWritable>{

        public void map(Text key, MapWritable value, Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");

            //SHIFT LEFT for sorting
            String modifiedThreeGram=threeGram[1]+" "+threeGram[2]+" "+threeGram[0];

            //<w1w2w3,data>
            context.write(new Text(modifiedThreeGram),value);

            //<*w2w3,data>//SHIFT LEFT for sorting
            String twoGramString=threeGram[0]+" "+threeGram[1]+" *";
            MapWritable map1=new MapWritable();
            IntWritable occurrences = (IntWritable) value.get(new Text("w1w2w3"));
            map1.put(new Text("occurrences"),occurrences);
            context.write(new Text(twoGramString),map1);

            //<*w2*,data>//SHIFT LEFT for sorting
            String oneGramString=threeGram[0]+" *"+" *";
            MapWritable map2=new MapWritable();
            occurrences = (IntWritable) value.get(new Text("w1w2w3"));
            map2.put(new Text("occurrences"),occurrences);
            context.write(new Text(oneGramString),map2);
        }
    }
    public static class CombinerClass
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;
            if(threeGram[2].equals("*")){
                //<*w2*,data>
                if(threeGram[1].equals("*")){
                    for(MapWritable map : values){
                        IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                        sum += occurrences.get();
                    }
                    MapWritable map1=new MapWritable();
                    map1.put(new Text("occurrences"),new IntWritable(sum));
                    context.write(key,map1);
                }
                else{
                    //<*w2w3,data>
                    for(MapWritable map : values){
                        IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                        sum += occurrences.get();
                    }
                    MapWritable map2=new MapWritable();
                    map2.put(new Text("occurrences"),new IntWritable(sum));
                    context.write(key,map2);
                }
            }
            else{
                //<w1w2w3,data>
                for(MapWritable map : values){//I HAVE ONLY ONE KEY OF 3GRAM
                    context.write(key,map);
                }
            }
        }
    }
    public static class ReducerClass
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        public void setup(Context context) throws IOException, InterruptedException {
            if(isSetUp.compareAndSet(false,true)){
                super.setup(context);
                w2w3Sum=context.getConfiguration().getInt("w2w3Sum",0);
                w2Sum=context.getConfiguration().getInt("w2Sum",0);
            }
        }

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;
            if(threeGram[2].equals("*")){
                //<*w2*,data>
                if(threeGram[1].equals("*")){
                    for(MapWritable map : values){
                        IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                        sum += occurrences.get();
                    }
                    w2Sum = sum;
                }
                else{
                    //<*w2w3,data>
                    for(MapWritable map : values){
                        IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                        sum += occurrences.get();
                    }
                    w2w3Sum = sum;
                }
            }
            else{
                //<w1w2w3,data>
                for(MapWritable map : values){//I HAVE ONLY ONE KEY OF 3GRAM
                    map.put(new Text("w2w3"),new IntWritable(w2w3Sum));
                    map.put(new Text("w2"),new IntWritable(w2Sum));
                    //SHIFT RIGHT for original string
                    String originalThreeGram=threeGram[2]+" "+threeGram[0]+" "+threeGram[1];
                    context.write(new Text(originalThreeGram),map);
                }
            }
        }
    }
    /*public static class PartitionerClass extends Partitioner<Text, MapWritable> {
        public int getPartition(Text key, MapWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }*/
    public static class PartitionerClass extends Partitioner<Text, MapWritable> {
        public int getPartition(Text key, MapWritable value, int numPartitions) {
            String []threeGram=(key.toString()).split(" ");
            String word = threeGram[0];
            return (word.hashCode()  & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step two");
        job.setJarByClass(StepTwo.class);
        job.setMapperClass(MapperClass.class);
        if(args[1].equals("on")){
            job.setCombinerClass(StepTwo.CombinerClass.class);
        }
        job.setReducerClass(StepTwo.ReducerClass.class);
        job.setPartitionerClass(StepTwo.PartitionerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}