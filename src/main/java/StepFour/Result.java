package StepFour;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Result implements WritableComparable<Result> {

    private Text word;
    private DoubleWritable prob;

    public Result(String s, double prob){
        word = new Text(s);
        this.prob=new DoubleWritable(prob);
    }
    public Result(){
        word = new Text();
        this.prob=new DoubleWritable();
    }


    @Override
    public int compareTo(Result o) {
        String threeGramOther = o.word.toString();
        String[] threeGramOthers = threeGramOther.split(" ");
        String wordOther=threeGramOthers[0] +" "+threeGramOthers[1];

        String threeGram = word.toString();
        String[] threeGrams = threeGram.split(" ");
        String word=threeGrams[0]+" "+threeGrams[1];
        int comp = word.compareTo(wordOther);
        if(comp==0){
            comp = o.prob.compareTo(prob);
        }
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        prob.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        prob.readFields(dataInput);
    }

    @Override
    public String toString(){
        return word.toString();
    }
}
