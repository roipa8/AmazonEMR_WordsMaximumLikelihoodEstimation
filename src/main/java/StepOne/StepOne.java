package StepOne;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;


public class StepOne {

    protected static int w1w2Sum = 0;
    protected static AtomicBoolean isSetUp = new AtomicBoolean(false);
    private static final char ALEPH = (char) 1488;
    private static final char TAV = (char) 1514;

    public static class MapperClass
            extends Mapper<LongWritable, Text, Text, MapWritable>{

        //Checks if the three gram is in hebrew and not empty
        protected boolean isValidThreeGram(String[] threeGram){
            if(threeGram.length != 3 ){
                return false;
            }
            for(String gram: threeGram) {
                for (int i = 0; i < gram.length(); i++) {
                    char c = gram.charAt(i);
                    if (c != ' ' && (c < ALEPH || c > TAV))
                        return false;
                }
            }
            return true;
        }

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            String[] threeGram = line[0].split(" ");

            //Checks if the input is valid ThreeGram
            if( isValidThreeGram(threeGram)){

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
    }

    public static class CombinerClass
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;

            //<w1w2*,data>
            if (threeGram[2].equals("*")) {
                for (MapWritable map : values) {
                    IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                    sum += occurrences.get();
                }
                MapWritable map1 = new MapWritable();
                map1.put(new Text("occurrences"), new IntWritable(sum));
                context.write(key, map1);
            }
            else{
                //<w1w2w3,data>
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("w1w2w3"));
                    sum += occurrences.get();
                }
                MapWritable map2=new MapWritable();
                map2.put(new Text("w1w2w3"),new IntWritable(sum));
                context.write(key,map2);
            }
        }
    }
        public static class ReducerClass
            extends Reducer<Text,MapWritable,Text,MapWritable> {
        //setup once the w1w2Sum var
        public  void setup(Context context) throws IOException, InterruptedException {
            if(isSetUp.compareAndSet(false,true)){
                super.setup(context);
                w1w2Sum=context.getConfiguration().getInt("w1w2Sum",0);
            }
        }

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] threeGram = key.toString().split(" ");
            int sum = 0;

            //<w1w2*,data>
            if(threeGram[2].equals("*")){
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("occurrences"));
                    sum += occurrences.get();
                }
                w1w2Sum = sum;
            }
            else{
                //<w1w2w3,data>
                for(MapWritable map : values){
                    IntWritable occurrences = (IntWritable) map.get(new Text("w1w2w3"));
                    sum += occurrences.get();
                }
                MapWritable map=new MapWritable();
                map.put(new Text("w1w2w3"),new IntWritable(sum));
                map.put(new Text("w1w2"),new IntWritable(w1w2Sum));
                context.write(key,map);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, MapWritable> {
        public int getPartition(Text key, MapWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "step one");
        job.setJarByClass(StepOne.class);
        job.setMapperClass(MapperClass.class);
        if(args[1].equals("on")){//Local aggregation mode
            job.setCombinerClass(StepOne.CombinerClass.class);
        }
        job.setReducerClass(StepOne.ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);//TODO CHANGE TO SequenceFileInputFormat
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}