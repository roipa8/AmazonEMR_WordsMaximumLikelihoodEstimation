package StepOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


public class StepOne {

    public static class MapperClass
            extends Mapper<Object, Text, Text, MapWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length==0){
                return;
            }
            String[] threeGram = line[0].split(" ");
            int occurrences = Integer.parseInt(line[2]);

            //<w1w2w3,data>
            String threeGramString=threeGram[0]+" "+threeGram[1]+" "+threeGram[2];
            MapWritable map1=new MapWritable();

            map1.put(new Text("w1w2w3"),new IntWritable(occurrences));

            context.write(new Text(threeGramString),map1);

            //<w1w2*,data>
            String twoGramString=threeGram[0]+" "+threeGram[1]+" "+"*";

            MapWritable map2=new MapWritable();

            map2.put(new Text("occurrences"),new IntWritable(occurrences));

            context.write(new Text(twoGramString),map2);
        }
    }

    public static class ReducerMap
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        private IntWritable w1w2Sum = new IntWritable(0);

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
                w1w2Sum.set(sum);
            }
            else{
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("w1w2w3"));
                    sum += occurrences.get();
                }

                MapWritable map=new MapWritable();

                map.put(new Text("w1w2w3"),new IntWritable(sum));
                map.put(new Text("w1w2"),new IntWritable(w1w2Sum.get()));

                context.write(key,map);

            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step one");
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(StepOne.ReducerMap.class);
        job.setReducerClass(StepOne.ReducerMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}