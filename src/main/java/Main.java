/*
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.log4j.BasicConfigurator;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {
    public static void main (String[]args){
        String localAggregation = (args.length > 0 && args[0].equals("-la")) ? "1" : "0";
        String bucket="s3://dspsbucket1";
        String jar="s1.jar";
        BasicConfigurator.configure();
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Region.US_EAST_1.toString())
                .withCredentials(credentials)
                .build();
//        StepFactory stepFactory = new StepFactory();
//
//        StepConfig enableDebugging = new StepConfig()
//                .withName("Enable Debugging")
//                .withActionOnFailure("TERMINATE_JOB_FLOW")
//                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(bucket+"/"+jar) // This should be a full map reduce application.
                .withMainClass("StepOne")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", bucket+"/output2/");

        StepConfig stepConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("RoiAndOmer")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname6")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0")
                .withLogUri(bucket+"/logs/");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
*/
