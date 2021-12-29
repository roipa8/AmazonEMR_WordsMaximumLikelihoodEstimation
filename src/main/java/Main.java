import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {
    public static void main (String[]args){
        String bucket="s3://dspsbucket1";

        BasicConfigurator.configure();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Region.US_EAST_1.toString())
                .withCredentials(credentials)
                .build();

       /* //StepOne
        HadoopJarStepConfig hadoopJarStepOne = new HadoopJarStepConfig()
                .withJar(bucket+"/"+"StepOne.jar")
                .withMainClass("StepOne")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", bucket+"/output1/");

        StepConfig stepConfigOne = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(hadoopJarStepOne)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
*/
        //StepTwo
        HadoopJarStepConfig hadoopJarStepTwo = new HadoopJarStepConfig()
                .withJar(bucket+"/"+"StepTwo.jar") //
                .withMainClass("StepTwo")
                .withArgs(bucket+"/output1/", bucket+"/output2/");

        StepConfig stepConfigTwo = new StepConfig()
                .withName("StepTwo")
                .withHadoopJarStep(hadoopJarStepTwo)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //StepThree
        HadoopJarStepConfig hadoopJarStepThree = new HadoopJarStepConfig()
                .withJar(bucket+"/"+"StepThree.jar") //
                .withMainClass("StepThree")
                .withArgs(bucket+"/output2/", bucket+"/output3/");

        StepConfig stepConfigThree = new StepConfig()
                .withName("StepThree")
                .withHadoopJarStep(hadoopJarStepThree)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //StepFour
        HadoopJarStepConfig hadoopJarStepFour = new HadoopJarStepConfig()
                .withJar(bucket+"/"+"StepFour.jar") //
                .withMainClass("StepFour")
                .withArgs(bucket+"/output3/", bucket+"/output4/");

        StepConfig stepConfigFour = new StepConfig()
                .withName("StepFour")
                .withHadoopJarStep(hadoopJarStepFour)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("RoiAndOmer")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Test2")
                .withInstances(instances)
                .withSteps(stepConfigTwo, stepConfigThree, stepConfigFour)//stepConfigOne, stepConfigTwo, stepConfigThree, stepConfigFour
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withLogUri(bucket+"/logs/");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
