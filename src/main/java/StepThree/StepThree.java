package StepThree;

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
import java.util.concurrent.atomic.AtomicBoolean;

public class StepThree {
    protected static int w3Sum = 0;
    protected static AtomicBoolean isSetUp = new AtomicBoolean(false);
    public static class MapperClass
            extends Mapper<Text, MapWritable, Text, MapWritable> {

        public void map(Text key, MapWritable value, Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");

            //SHIFT RIGHT WITH ENGLISH!! TODO SHIFT LEFT WITH HEB
            String modifiedThreeGram=threeGram[2]+" "+threeGram[0]+" "+threeGram[1];
            //<w1w2w3,data>
            context.write(new Text(modifiedThreeGram),value);


            //<**w3,data>//SHIFT RIGHT WITH ENGLISH!! TODO SHIFT LEFT WITH HEB
            String oneGramString=threeGram[0]+" *"+" *";

            MapWritable map=new MapWritable();
            IntWritable occurrences = (IntWritable) value.get(new Text("w1w2w3"));
            map.put(new Text("occurrences"),occurrences);

            context.write(new Text(oneGramString),map);
        }
    }

    public static class ReducerMap
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        public  void setup(Context context) throws IOException, InterruptedException {
            if(isSetUp.compareAndSet(false, true)){
                super.setup(context);
                w3Sum=context.getConfiguration().getInt("w3Sum",0);
            }
        }

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;
            if(threeGram[2].equals("*")){
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                    sum += occurrences.get();
                }
                w3Sum = sum;
            }
            else{
                for(MapWritable map : values){//I HAVE ONLY ONE KEY OF 3GRAM
                    map.put(new Text("w3"),new IntWritable(w3Sum));
                    //SHIFT LEFT WITH ENGLISH!! TODO SHIFT RIGHT WITH HEB
                    String originalThreeGram=threeGram[1]+" "+threeGram[2]+" "+threeGram[0];
                    context.write(new Text(originalThreeGram),map);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step three");
        job.setJarByClass(StepThree.class);
        job.setMapperClass(StepThree.MapperClass.class);
        job.setCombinerClass(StepThree.ReducerMap.class);
        job.setReducerClass(StepThree.ReducerMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
