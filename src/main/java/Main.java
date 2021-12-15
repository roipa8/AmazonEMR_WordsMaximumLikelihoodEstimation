import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {
    public static void main (String[]args){
        String bucket="s3://dspsbucket1";
        String jar="";
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Region.US_EAST_1.toString())
                .withCredentials(credentials)
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(bucket+"/"+jar) // This should be a full map reduce application.
                .withMainClass("StepOne")
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");

        StepConfig stepConfig = new StepConfig()
                .withName("StepOne")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4_LARGE.toString())
                .withSlaveInstanceType(InstanceType.M4_LARGE.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
