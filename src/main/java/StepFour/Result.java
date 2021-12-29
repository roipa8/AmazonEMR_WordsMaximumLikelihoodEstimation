package StepFour;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Result implements WritableComparable<Result> {

    private Text word;
    private double prob;

    public Result(String s, double prob){
        word = new Text(s);
        this.prob=prob;
    }


    @Override
    public int compareTo(Result o) {
        String threeGramOther = o.word.toString();
        String[] threeGramOthers = threeGramOther.split(" ");
        String firstWordOther=threeGramOthers[0];
        String secondWordOther=threeGramOthers[1];
        double probOther = o.prob;
//        String threeGram = o.word.toString();
//        String[] threeGramOthers = threeGramOther.split(" ");
//        String firstWordOther=threeGramOthers[0];
//        String secondWordOther=threeGramOthers[1];
//        double probOther = o.prob;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
