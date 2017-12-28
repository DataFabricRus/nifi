package org.apache.nifi.processors.gcp.dataflow;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.gcp.dataflow.service.GCPDataflowService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.*;

public class CreateGCDataflowJobFromTeplateTest {

    private TestRunner controller;

    private Dataflow dataflowService;

    private GCPDataflowService dataflowServiceProvider;

    private Dataflow.Projects.Locations.Templates.Launch launch;

    private LaunchTemplateResponse launchTemplateRequest;


    @Before
    public void init() throws IOException, InitializationException {
        dataflowService = mock(Dataflow.class, Mockito.RETURNS_DEEP_STUBS);
        launch = mock(Dataflow.Projects.Locations.Templates.Launch.class);
        launchTemplateRequest = mock(LaunchTemplateResponse.class);

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

        when(launch.execute()).thenReturn(launchTemplateRequest);
        //when(launch.getValidateOnly()).thenReturn(true);
    }

    @Test
    public void testJobRunning() throws IOException {
        Job job_1 = mock(Job.class);
        Job job_2 = mock(Job.class);


        Dataflow.Projects.Locations.Jobs.Get getJobRequest_1 = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                eq("id_1"))
        ).thenReturn(getJobRequest_1);
        when(getJobRequest_1.execute()).thenReturn(job_1);
        when(job_1.getCurrentState()).thenReturn(null,
                "JOB_STATE_PENDING",
                "JOB_STATE_RUNNING",
                "JOB_STATE_DONE"
        );
        when(job_1.getId()).thenReturn("id_1");


        Dataflow.Projects.Locations.Jobs.Get getJobRequest_2 = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                eq("id_2"))
        ).thenReturn(getJobRequest_2);
        when(getJobRequest_2.execute()).thenReturn(job_2);
        when(job_2.getCurrentState()).thenReturn(null,
                "JOB_STATE_PENDING",
                "JOB_STATE_RUNNING",
                "JOB_STATE_CANCELLING",
                "JOB_STATE_CANCELLED"
        );
        when(job_2.getId()).thenReturn("id_2");


        final List<MockFlowFile> succeeded;


        controller.addConnection(CreateGCDataflowJobFromTeplate.REL_INPROCESS);
        controller.enqueue("");
        when(launchTemplateRequest.getJob()).thenReturn(job_1);
        controller.run();

        controller.enqueue("");
        when(launchTemplateRequest.getJob()).thenReturn(job_2);
        controller.run();


        controller.enqueue(controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_INPROCESS).get(0));
        controller.run();

        for (FlowFile flowFile : controller.getFlowFilesForRelationship(CreateGCDataflowJobFromTeplate.REL_INPROCESS)) {
            System.out.println(flowFile.getAttribute(CreateGCDataflowJobFromTeplate.JOB_STATE_ATTR));
        }
    }

    public class MockGCPDataflowService extends AbstractControllerService implements GCPDataflowService {
        @Override
        public Dataflow getDataflowService() {
            return dataflowService;
        }
    }


}
