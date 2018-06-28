package org.apache.nifi.processors.gcp.dataflow;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CreateGCDataflowJobFromTeplateTest {

    private TestRunner controller;

    private Dataflow dataflowService;

    private GCPDataflowService dataflowServiceProvider;

    private Dataflow.Projects.Locations.Templates.Launch launch;

    private LaunchTemplateResponse launchTemplateResponse;

    //reference values
    private static final String JOB_NAME_VALUE = "job name";
    private static final String JOB_TEMPLATE_PATH_VALUE = "/path/to/template";

    private Random random = new Random();

    @Before
    public void init() throws IOException, InitializationException {
        dataflowService = mock(Dataflow.class, Mockito.RETURNS_DEEP_STUBS);
        launch = mock(Dataflow.Projects.Locations.Templates.Launch.class);
        launchTemplateResponse = mock(LaunchTemplateResponse.class);

        dataflowServiceProvider = new MockGCPDataflowService();


        controller = TestRunners.newTestRunner(LaunchAndGetGCPJob.class);

        controller.addControllerService("service", dataflowServiceProvider);
        controller.enableControllerService(dataflowServiceProvider);

        controller.setProperty(LaunchAndGetGCPJob.DATAFLOW_SERVICE, "service");
        controller.setProperty(LaunchAndGetGCPJob.PROJECT_ID, "project name");
        controller.setProperty(LaunchAndGetGCPJob.JOB_NAME, JOB_NAME_VALUE);
        controller.setProperty(LaunchAndGetGCPJob.GCS_PATH, JOB_TEMPLATE_PATH_VALUE);

        when(dataflowService.projects().locations().templates().launch(
                any(String.class),
                any(String.class),
                any(LaunchTemplateParameters.class))
        ).thenReturn(launch);

        when(launch.execute()).thenReturn(launchTemplateResponse);


        controller.addConnection(LaunchAndGetGCPJob.REL_INPROCESS);
        controller.addConnection(LaunchAndGetGCPJob.REL_NOTIFY);
        controller.addConnection(LaunchAndGetGCPJob.REL_FAILURE);
        controller.addConnection(LaunchAndGetGCPJob.REL_SUCCESS);
        //when(launch.getValidateOnly()).thenReturn(true);
    }

    @Test
    public void testJobDoneAndChain() throws IOException {
        //mock the job
        String jobID = "id_1";
        Job jobMock = mock(Job.class);
        when(jobMock.getId()).thenReturn(jobID);
        when(jobMock.getCurrentState()).thenReturn("JOB_STATE_DONE");

        //mock incoming flow file
        MockFlowFile incomingFlowFile = new MockFlowFile(random.nextLong());
        Map<String, String> attribures = new HashMap<>();
        attribures.put(LaunchAndGetGCPJob.JOB_ID_ATTR, jobID);
        attribures.put(LaunchAndGetGCPJob.JOB_NAME_ATTR, JOB_NAME_VALUE);
        incomingFlowFile.putAttributes(attribures);

        //mock the Dataflow service
        Dataflow.Projects.Locations.Jobs.Get getJobRequest = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                eq(jobID))
        ).thenReturn(getJobRequest);
        when(getJobRequest.execute()).thenReturn(jobMock);

        //init processor for the given test
        controller.setProperty(LaunchAndGetGCPJob.VALIDATE_ONLY, "false");
        //push some flow file to run the processor
        controller.enqueue(incomingFlowFile);

        //start processor
        controller.run();

        List<MockFlowFile> successes = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_SUCCESS);
        assertEquals(1, successes.size());
        MockFlowFile success = successes.get(0);
        assertNull(success.getAttribute(LaunchAndGetGCPJob.JOB_ID_ATTR));
        assertNull(success.getAttribute(LaunchAndGetGCPJob.JOB_NAME_ATTR));

        List<MockFlowFile> notifications = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_NOTIFY);
        assertEquals(1, notifications.size());
        MockFlowFile notification = notifications.get(0);
        String content = new String(controller.getContentAsByteArray(notification));
        NotificationMessage notificationMessage = (new ObjectMapper()).readerFor(NotificationMessage.class).readValue(content);
        assertEquals(LaunchAndGetGCPJob.JOB_STATE_DONE, notificationMessage.getState());


        controller.clearTransferState();


        jobLaunch("id_1", success);
    }


    @Test
    public void testJobLaunch() throws IOException {
        jobLaunch("id_1", new MockFlowFile(random.nextLong()));
    }

    private void jobLaunch(String jobID, MockFlowFile incomingFlowFile) throws IOException {
        Job jobMock = mock(Job.class);
        when(jobMock.getId()).thenReturn(jobID);
        when(launchTemplateResponse.getJob()).thenReturn(jobMock);

        controller.setProperty(LaunchAndGetGCPJob.VALIDATE_ONLY, "false");
        //push some flow file to run the processor
        controller.enqueue(incomingFlowFile);

        controller.run();

        List<MockFlowFile> notifications = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_NOTIFY);
        assertTrue(!notifications.isEmpty());

        for (MockFlowFile flowFile : notifications) {
            String content = new String(controller.getContentAsByteArray(flowFile));
            NotificationMessage notificationMessage = (new ObjectMapper()).readerFor(NotificationMessage.class).readValue(content);
            assertEquals(LaunchAndGetGCPJob.JOB_STATE_ON_START, notificationMessage.getState());
            assertEquals(JOB_TEMPLATE_PATH_VALUE, notificationMessage.getFields().get(LaunchAndGetGCPJob.JOB_TEMPLATE_PATH_NF_FIELD));
            assertEquals(Boolean.FALSE.toString(), notificationMessage.getFields().get(LaunchAndGetGCPJob.JOB_DRY_RUN_NF_FIELD));
            assertEquals(JOB_NAME_VALUE, notificationMessage.getFields().get(LaunchAndGetGCPJob.JOB_NAME_NF_FIELD));
        }

        //check that processor has launched and pushed flow file to 'inprocess' queue to use it for subsequent job polling
        List<MockFlowFile> inProcesses = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS);
        assertTrue(!inProcesses.isEmpty());
        for (MockFlowFile flowFile : inProcesses) {
            assertEquals(jobID, flowFile.getAttribute(LaunchAndGetGCPJob.JOB_ID_ATTR));
            assertEquals(JOB_NAME_VALUE, flowFile.getAttribute(LaunchAndGetGCPJob.JOB_NAME_ATTR));
        }

        List<MockFlowFile> failures = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_FAILURE);
        assertTrue(failures.isEmpty());

        List<MockFlowFile> successes = controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_SUCCESS);
        assertTrue(successes.isEmpty());
    }


    @Ignore
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


        controller.addConnection(LaunchAndGetGCPJob.REL_INPROCESS);
        controller.enqueue("");
        when(launchTemplateResponse.getJob()).thenReturn(job_1);
        controller.run();

        controller.enqueue("");
        when(launchTemplateResponse.getJob()).thenReturn(job_2);
        controller.run();


        controller.enqueue(controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS).get(0));
        controller.run();

        controller.enqueue(controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS).get(0));
        controller.run();

        for (FlowFile flowFile : controller.getFlowFilesForRelationship(LaunchAndGetGCPJob.REL_INPROCESS)) {
            System.out.println(flowFile.getAttribute(LaunchAndGetGCPJob.JOB_STATE_ATTR));
        }
    }

    public class MockGCPDataflowService extends AbstractControllerService implements GCPDataflowService {
        @Override
        public Dataflow getDataflowService() {
            return dataflowService;
        }
    }


}
