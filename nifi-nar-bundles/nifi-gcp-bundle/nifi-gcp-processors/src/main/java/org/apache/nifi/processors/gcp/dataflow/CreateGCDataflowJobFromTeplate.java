package org.apache.nifi.processors.gcp.dataflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.dataflow.service.GCPDataflowService;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;


@Tags({"google", "google cloud", "dataflow", "put"})
@CapabilityDescription("Creates dataflow job from a template.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the id of a job"),
        @WritesAttribute(attribute = "projectId", description = "The id of the project"),
        @WritesAttribute(attribute = "jobId", description = "The id of the job"),
        @WritesAttribute(attribute = "output", description = "The job's output directory")
})

@DynamicProperty(name = "The name of a User-Defined Parameters to be sent to job",
        value = "The value of a User-Defined parameter to be set to job",
        description = "Allows User-Defined parameters to be sent to job as key/value pairs",
        supportsExpressionLanguage = true)
public class CreateGCDataflowJobFromTeplate extends AbstractProcessor {
    //TODO: think about the name. It it can be called an Ingress Processor then possibly it would be better to start name from Listen. E.g. ListenGCDJobREST

    private Map<String, List<String>> ids = new HashMap<>();


    // Identifies the distributed map cache client
    public static final PropertyDescriptor DATAFLOW_SERVICE = new PropertyDescriptor.Builder()
            .name("dataflow-service-provider")
            .displayName("Dataflow Service Provider")
            .description("The Controller Service that is used to provide dataflow service")
            .required(true)
            .identifiesControllerService(GCPDataflowService.class)
            .build();

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor
            .Builder().name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor
            .Builder().name("job-name")
            .displayName("Job name")
            .description("Name of the created job")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor GCS_PATH = new PropertyDescriptor
            .Builder().name("gcs-path")
            .displayName("GCS path")
            .description("Google Cloud Storage path to the job template")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor VALIDATE_ONLY = new PropertyDescriptor
            .Builder().name("validate-only")
            .displayName("Validate only attribute")
            .description("If true, the request is validated but not actually executed. Default is true")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor LOCATION = new PropertyDescriptor
            .Builder().name("location")
            .displayName("Job location")
            .description("Enables to specify where it is necessary to place the job")
            .required(true)
            .allowableValues("europe-west1", "us-central1", "us-west1")
            .defaultValue("europe-west1")
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .add(DATAFLOW_SERVICE)
                .add(PROJECT_ID)
                .add(LOCATION)
                .add(JOB_NAME)
                .add(GCS_PATH)
                .add(VALIDATE_ONLY)
                .build();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google Cloud Dataflow launch operation.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship after a failure on Google Cloud Dataflow launch operation.")
                    .build();

    public static final Relationship REL_NOTIFY =
            new Relationship.Builder().name("notify")
                    .description("FlowFiles with job status information are routed to this relationship while Google Cloud Dataflow launch operation.")
                    .build();

    public static Set<Relationship> relationships;


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_NOTIFY);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;
        try {
            Dataflow dataflowService = context
                    .getProperty(DATAFLOW_SERVICE)
                    .asControllerService(GCPDataflowService.class)
                    .getDataflowService();

            flowFile = session.get();


            if (flowFile == null) {
                examineJobs(context, session, dataflowService);
            } else {
                launchJob(dataflowService, context, session, flowFile);
                examineJobs(context, session, dataflowService);
            }

        } catch (final Exception e) {
            getLogger().error("Failed to launch job due to ", e);
            if (flowFile != null) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void launchJob(
            Dataflow dataflowService,
            ProcessContext context,
            ProcessSession session,
            FlowFile flowFile
    ) throws IOException {


        Map<String, String> parameters = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                final String value = context
                        .getProperty(entry.getKey())
                        .evaluateAttributeExpressions(flowFile)
                        .getValue();
                parameters.put(entry.getKey().getName(), value);
            }
        }

        LaunchTemplateParameters launchTemplateParameters = new LaunchTemplateParameters();

        String jobName = context.getProperty(JOB_NAME)
                .evaluateAttributeExpressions(flowFile)
                .getValue();

        launchTemplateParameters.setJobName(jobName);
        launchTemplateParameters.setParameters(parameters);


        String projectId = context.getProperty(PROJECT_ID)
                .evaluateAttributeExpressions(flowFile)
                .getValue();


        Dataflow.Projects.Locations.Templates.Launch launch = dataflowService
                .projects()
                .locations()
                .templates()
                .launch(projectId, context.getProperty(LOCATION).getValue(), launchTemplateParameters);


        String gcpPath = context.getProperty(GCS_PATH)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        launch.setGcsPath(gcpPath);

        launch.setValidateOnly(context.getProperty(VALIDATE_ONLY).asBoolean());

        LaunchTemplateResponse launchResponse = launch.execute();

        if (!launch.getValidateOnly()) {
            if (launchResponse != null && launchResponse.getJob() != null && launchResponse.getJob().getId() != null) {
                List<String> jobParameters = new ArrayList<>();
                jobParameters.add(projectId);
                jobParameters.add(gcpPath);
                jobParameters.add(jobName);
                ids.put(launchResponse.getJob().getId(), jobParameters);
            } else {
                throw new ProcessException("Get null while job launching");
            }
        }
        notifyLaunchPreparation(context, session, parameters, flowFile);
    }

    private void notifyLaunchPreparation(
            ProcessContext context,
            ProcessSession session,
            Map<String, String> parameters,
            FlowFile flowFile) {
        try {
            Map<String, String> attributes = new HashMap<>();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field("job name", context.getProperty(JOB_NAME).evaluateAttributeExpressions(flowFile).getValue()));
            fields.add(new Field("template path", context.getProperty(GCS_PATH).evaluateAttributeExpressions(flowFile).getValue()));
            fields.add(new Field("dry run", context.getProperty(VALIDATE_ONLY).getValue()));
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                fields.add(new Field(entry.getKey(), entry.getValue()));
            }
            Message message = new Message();
            message.setFallback("Job with name " + context.getProperty(JOB_NAME).evaluateAttributeExpressions(flowFile).getValue() + " is launched!");
            message.setPretext(":hotsprings: Job launch prepared");
            message.setTitle("Job is going to be launched from a template with the following parameters:");
            message.setFields(fields);
            message.setColor(MessageColor.DANGER.color);
            attributes.put("attachments", message.toString());

            attributes.put("attachments", (new ObjectMapper()).writeValueAsString(message));
            if (attributes.size() > 0) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_NOTIFY);

        } catch (Exception e) {
            getLogger().error("Can't create notification!", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void notifyJobState(
            ProcessSession session,
            String id,
            String state) {
        FlowFile flowFile = null;
        try {
            flowFile = session.create();
            Map<String, String> attributes = new HashMap<>();
            List<Field> fields = new ArrayList<>();
            List<String> jobParameters = ids.get(id);
            fields.add(new Field("job name", jobParameters.get(2)));
            fields.add(new Field("job id", id));
            fields.add(new Field("job state", state));
            Message message = new Message();
            message.setFallback("Job with id " + id + " is in state " + state);
            message.setTitle("Job report details:");
            switch (state) {
                case "JOB_STATE_RUNNING":
                case "JOB_STATE_PENDING":
                case "JOB_STATE_STOPPED":
                    message.setPretext(":warning: Job in action report");
                    message.setColor(MessageColor.WARNING.color);
                    break;
                case "JOB_STATE_DONE":
                    message.setPretext(":white_check_mark: Job done report");
                    message.setColor(MessageColor.GOOD.color);
                    break;
                case "JOB_STATE_CANCELLED":
                    message.setPretext(":exclamation: Job termination report");
                    message.setColor(MessageColor.DANGER.color);
                    break;
                default:
                    message.setPretext(":exclamation: Job unworkable report");
                    message.setColor(MessageColor.DANGER.color);
                    break;

            }
            message.setFields(fields);
            attributes.put("attachments", (new ObjectMapper()).writeValueAsString(message));
            if (attributes.size() > 0) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_NOTIFY);
        } catch (Exception e) {
            getLogger().error("Can't create notification!", e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void examineJobs(
            ProcessContext context,
            ProcessSession session,
            Dataflow dataflowService
    ) throws IOException {
        Iterator<String> iterator = ids.keySet().iterator();
        while (iterator.hasNext()) {
            String id = iterator.next();

            List<String> jobParameters = ids.get(id);

            Dataflow.Projects.Locations.Jobs.Get request = dataflowService.projects().locations().jobs().get(
                    jobParameters.get(0),
                    context.getProperty(LOCATION).getValue(),
                    id
            );

            Job response = request.execute();


            String state = response.getCurrentState();

            if (state == null) {
                //TODO: count attempts?
                continue;
            }

            switch (state) {
                case "JOB_STATE_RUNNING":
                case "JOB_STATE_PENDING":
                case "JOB_STATE_CANCELLING":
                    notifyJobState(session, id, state);
                    break;
                case "JOB_STATE_DONE": {
                    notifyJobState(session, id, state);
                    FlowFile flowFile = session.create();
                    buildFlowFile(context, session, flowFile, id);
                    session.getProvenanceReporter().create(flowFile, "It takes something about"
                            + response.getCurrentStateTime()
                            + " to perform the job");
                    session.transfer(flowFile, REL_SUCCESS);
                    getLogger().info("The job with id " + id + " has been done!");
                    iterator.remove();
                    break;
                }
                case "JOB_STATE_UNKNOWN":
                case "JOB_STATE_FAILED":
                case "JOB_STATE_CANCELLED": {
                    notifyJobState(session, id, state);
                    FlowFile flowFile = session.create();
                    buildFlowFile(context, session, flowFile, id);
                    session.transfer(flowFile, REL_FAILURE);
                    getLogger().info("The job with id {} unsuccessfully terminated with the state {}!", new Object[]{id, state});
                    iterator.remove();
                    break;
                }
                case "JOB_STATE_STOPPED":
                    notifyJobState(session, id, state);
                    getLogger().info("The job with id " + id + " has been stopped!");
                    break;
                default:
                    notifyJobState(session, id, state);
                    getLogger().error("The job with id {} is in unworkable {} state!", new Object[]{id, state});
                    iterator.remove();
            }
        }
    }


    private FlowFile buildFlowFile(
            ProcessContext context,
            ProcessSession session,
            FlowFile flowFile,
            String id
    ) {
        List<String> jobParameters = ids.get(id);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(JOB_NAME.getName(), jobParameters.get(2));
        attributes.put(VALIDATE_ONLY.getName(), context.getProperty(VALIDATE_ONLY).getValue());
        attributes.put(GCS_PATH.getName(), jobParameters.get(1));
        if (attributes.size() > 0) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }
        return flowFile;
    }


    private class Field implements Serializable {
        private String title;
        private String value;

        public Field(String title, String value) {
            this.title = title;
            this.value = value;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    enum MessageColor {
        DANGER("danger"),
        WARNING("warning"),
        GOOD("good");

        private String color;

        MessageColor(String color) {
            this.color = color;
        }

    }

    private class Message implements Serializable {


        private String fallback;
        private String pretext;
        private List<Field> fields;
        private String title;
        private String color;

        public String getFallback() {
            return fallback;
        }

        public void setFallback(String fallback) {
            this.fallback = fallback;
        }

        public String getPretext() {
            return pretext;
        }

        public void setPretext(String pretext) {
            this.pretext = pretext;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getColor() {
            return color;
        }

        public void setColor(String color) {
            this.color = color;
        }

        public List<Field> getFields() {
            return fields;
        }

        public void setFields(List<Field> fields) {
            this.fields = fields;
        }


    }

// JOB_STATE_UNKNOWN
// JOB_STATE_STOPPED
// JOB_STATE_RUNNING
// JOB_STATE_DONE
// JOB_STATE_FAILED
// JOB_STATE_CANCELLED
// JOB_STATE_UPDATED
// JOB_STATE_DRAINING
// JOB_STATE_DRAINED
// JOB_STATE_PENDING
// JOB_STATE_CANCELLING

}
