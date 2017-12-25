package org.apache.nifi.processors.gcp.dataflow;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.gcp.dataflow.service.GCPDataflowService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

public class CreateGCDataflowJobFromTeplateTest {

    private TestRunner controller;

    private Dataflow dataflowService;

    private GCPDataflowService dataflowServiceProvider;

    private Dataflow.Projects.Locations.Templates.Launch launch;

    private LaunchTemplateResponse launchTemplateRequest;

    private Dataflow.Projects.Locations.Jobs.Get getJobRequest;

    private Job initialJob;
    private Job followingJob;

    @Before
    public void init() throws IOException, InitializationException {
        dataflowService = mock(Dataflow.class, Mockito.RETURNS_DEEP_STUBS);
        launch = mock(Dataflow.Projects.Locations.Templates.Launch.class);
        launchTemplateRequest = mock(LaunchTemplateResponse.class);
        getJobRequest = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        initialJob = mock(Job.class);
        followingJob = mock(Job.class);
        dataflowServiceProvider = new MockGCPDataflowService();


        controller = TestRunners.newTestRunner(CreateGCDataflowJobFromTeplate.class);

        controller.addControllerService("service", dataflowServiceProvider);
        controller.enableControllerService(dataflowServiceProvider);

        controller.setProperty(CreateGCDataflowJobFromTeplate.DATAFLOW_SERVICE, "service");
        controller.setProperty(CreateGCDataflowJobFromTeplate.PROJECT_ID, "project");
        controller.setProperty(CreateGCDataflowJobFromTeplate.JOB_NAME, "name");
        controller.setProperty(CreateGCDataflowJobFromTeplate.GCS_PATH, "path");


        when(dataflowService.projects().locations().templates().launch(
                any(String.class),
                any(String.class),
                any(LaunchTemplateParameters.class))
        ).thenReturn(launch);

        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                any(String.class))
        ).thenReturn(getJobRequest);


        List<Job> jobs = new ArrayList<>();


        when(launch.execute()).thenReturn(launchTemplateRequest);

    }

    @Test
    public void testJobRunning() throws IOException {

        String[] states = {null, "JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_DONE"};

        List<Job> jobs = Arrays
                .stream(states)
                .map(x -> {
                    Job mock = mock(Job.class);
                    when(mock.getCurrentState()).thenReturn(x);
                    when(mock.getId()).thenReturn("id");
                    return mock;
                }).collect(Collectors.toList());
        when(launchTemplateRequest.getJob()).thenReturn(jobs.remove(0));
        when(getJobRequest.execute()).thenReturn(jobs.remove(0), jobs.toArray(new Job[jobs.size()]));

        when(initialJob.getCurrentState()).thenReturn(null);
        when(initialJob.getId()).thenReturn("id");

        when(followingJob.getCurrentState()).thenReturn(
                "JOB_STATE_PENDING",
                "JOB_STATE_RUNNING",
                "JOB_STATE_DONE"
        );
        when(followingJob.getId()).thenReturn("id");


        controller.enqueue("");

        controller.run();


        for (FlowFile flowFile : controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_NOTIFY)) {
            System.out.println(flowFile.getAttribute("attachments"));
        }
    }

    public class MockGCPDataflowService extends AbstractControllerService implements GCPDataflowService{
        @Override
        public Dataflow getDataflowService() {
            return dataflowService;
        }
    }
}
