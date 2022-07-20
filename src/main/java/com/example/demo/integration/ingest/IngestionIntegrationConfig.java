package com.example.demo.integration.ingest;

import com.amazonaws.services.s3.AmazonS3;
import com.example.demo.integration.FireOnceTrigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.annotation.*;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import java.io.File;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;

@Configuration
@Slf4j
@Profile("ingestion")
public class IngestionIntegrationConfig {

    private static final String FILE = "file",
            EXECUTION_TIME = "execution_time";

    private final AmazonS3 amazonS3;
    private final JobLauncher jobLauncher;
    private final Job job;
    private final String outputBucketName;

    public IngestionIntegrationConfig(AmazonS3 amazonS3, JobLauncher jobLauncher, @Qualifier("ingestJob") Job job, @Value("${aws.s3.outputBucketName}")String outputBucketName) {
        this.amazonS3 = amazonS3;
        this.jobLauncher = jobLauncher;
        this.job = job;
        this.outputBucketName = outputBucketName;
    }

    public void exit(String message) {
        exit(message, null);
    }

    public void exit(String message, Exception ex) {
        if (null != ex) {
            log.error(message, ex);
        } else {
            log.info(message);
        }
        System.exit(0); // kills the JVM when all integration steps are finished, this allows single run scheduling
    }

    @Bean
    public FireOnceTrigger fireOnceTrigger() {
        return new FireOnceTrigger();
    }

    @Bean(name = "s3InputChannel")
    public PollableChannel s3InputChannel() {
        return new QueueChannel();
    }

    @Bean
    public S3InboundFileSynchronizer inboundFileSynchronizer() {
        S3InboundFileSynchronizer synchronizer = new S3InboundFileSynchronizer(amazonS3);
        synchronizer.setDeleteRemoteFiles(true);
        synchronizer.setPreserveTimestamp(true);
        synchronizer.setRemoteDirectory(outputBucketName);
        return synchronizer;
    }

    @Bean
    @InboundChannelAdapter(value="s3InputChannel", poller = @Poller(trigger = "fireOnceTrigger"))
    public MessageSource<List<File>> s3MessageSource(S3InboundFileSynchronizer synchronizer) {
        return () -> {
            log.info("Syncing S3 objects in [{}] bucket", outputBucketName);
            File output = new File("output");
            synchronizer.synchronizeToLocalDirectory(output);
            return new GenericMessage<>(asList(output.listFiles()));
        };
    }

    @Filter(inputChannel = "s3InputChannel", outputChannel = "fileSplitterChannel", discardChannel = "noFilesChannel")
    public boolean emptyFilesFilter(List<File> files) {
        return !files.isEmpty();
    }

    @Splitter(inputChannel = "fileSplitterChannel", outputChannel = "fileChannel")
    public List<File> fileSplitter(List<File> files) {
        log.info("Splitting [{}] files", files.size());
        return files;
    }

    @Transformer(inputChannel = "fileChannel", outputChannel = "launchJobChannel")
    public JobParameters fileToJobParametersTransformer(File file) {
        log.info("Building job parameters for file [{}]", file);
        return new JobParametersBuilder()
                .addString(FILE, file.getAbsolutePath())
                .addDate(EXECUTION_TIME, new Date())
                .toJobParameters();
    }

    @ServiceActivator(inputChannel = "launchJobChannel", outputChannel = "deleteLocalFileChannel")
    public File launchExtractJob(JobParameters jobParameters) throws JobExecutionException {
        log.info("Launching ingestJob with parameters [{}]", jobParameters);
        JobExecution jobExecution = jobLauncher.run(job, jobParameters);
        log.info("Job execution [{}] ended at [{}] with status [{}] for parameters [{}]",
                jobExecution.getId(), jobExecution.getEndTime(), jobExecution.getStatus(), jobExecution.getJobParameters());
        return new File(jobParameters.getString(FILE));
    }

    @ServiceActivator(inputChannel = "deleteLocalFileChannel", outputChannel = "endChannel")
    public Boolean deleteLocalFile(File file) {
        log.info("Deleting file [{}]", file.getName());
        return file.delete();
    }

    @ServiceActivator(inputChannel = "noFilesChannel")
    public void noFiles() {
        exit("No files found to process, shutting down JVM");
    }

    @Aggregator(inputChannel = "endChannel")
    public void shutdown() {
        exit("Integration flow complete, shutting down JVM");
    }

    @ServiceActivator(inputChannel = "errorChannel")
    public void handleError(Exception ex) {
        exit("Error occurred in integration flow, shutting down JVM", ex);
    }

}
