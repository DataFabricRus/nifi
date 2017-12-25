package org.apache.nifi.processors.gcp.dataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.concurrent.TimeUnit;


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

    private List<String> ids = new ArrayList<>();


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
            .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor
            .Builder().name("job-name")
            .displayName("Job name")
            .description("Name of the created job")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GCS_PATH = new PropertyDescriptor
            .Builder().name("gcs-path")
            .displayName("GCS path")
            .description("Google Cloud Storage path to the job template")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALIDATE_ONLY = new PropertyDescriptor
            .Builder().name("validate-only")
            .displayName("Validate only attribute")
            .description("If true, the request is validated but not actually executed. Default is true")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .add(DATAFLOW_SERVICE)
                .add(PROJECT_ID)
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

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            Dataflow dataflowService = context
                    .getProperty(DATAFLOW_SERVICE)
                    .asControllerService(GCPDataflowService.class)
                    .getDataflowService();

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
            launchTemplateParameters.setJobName(context.getProperty(JOB_NAME).getValue());
            launchTemplateParameters.setParameters(parameters);


            String projectId = context.getProperty(PROJECT_ID).getValue();
            Dataflow.Projects.Locations.Templates.Launch launch = dataflowService
                    .projects()
                    .locations()
                    .templates()
                    .launch(projectId, "europe-west1", launchTemplateParameters);


            launch.setGcsPath(context.getProperty(GCS_PATH).getValue());
            launch.setValidateOnly(context.getProperty(VALIDATE_ONLY).asBoolean());

            notifyLaunchPreparation(context, session, parameters);

            final long startNanos = System.nanoTime();
            LaunchTemplateResponse launchResponse = launch.execute();


            //-----------------

            if (launch.getValidateOnly()) {
                notifyJobState(context, session, "There is no id. It is a dry run", "JOB_DRY_RUN");
            } else {
                wetRun(context, session, dataflowService, launchResponse);
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            buildFlowFile(context, session, parameters, flowFile);
            session.getProvenanceReporter().create(flowFile, "It takes "
                    + millis
                    + " to perform the job");
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("The report about the job is presented with the flow file {}", new Object[]{flowFile});
            session.commit();

        } catch (final Exception e) {
            getLogger().error("Failed to launch job due to ", e);
            if (flowFile != null) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }

    }

    private void launchJob(){

    }

    private void notifyLaunchPreparation(ProcessContext context, ProcessSession session, Map<String, String> parameters) {
        FlowFile flowFile = null;
        try {
            flowFile = session.create();
            Map<String, String> attributes = new HashMap<>();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field("job name", context.getProperty(JOB_NAME).getValue()));
            fields.add(new Field("template path", context.getProperty(GCS_PATH).getValue()));
            fields.add(new Field("dry run", context.getProperty(VALIDATE_ONLY).getValue()));
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                fields.add(new Field(entry.getKey(), entry.getValue()));
            }
            Message message = new Message();
            message.setFallback("Job with name " + context.getProperty(JOB_NAME).getValue() + " is launched!");
            message.setPretext(":hotsprings: Job launch prepared");
            message.setTitle("Job is going to be launched from a template with the following parameters:");
            message.setFields(fields);
            message.setColor(MessageColor.DANGER.color);
            attributes.put("attachments", message.toString());

            try {
                attributes.put("attachments", (new ObjectMapper()).writeValueAsString(message));
                if (attributes.size() > 0) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }
                session.transfer(flowFile, REL_NOTIFY);
            } catch (JsonProcessingException e) {
                getLogger().error("Can't create notification!", e);
                session.remove(flowFile);
            }
        } catch (Exception e) {
            getLogger().error("Can't create notification!", e);
            if (flowFile != null) {
                session.remove(flowFile);
            }
        }
    }

    private void notifyJobState(
            ProcessContext context,
            ProcessSession session,
            String id,
            String state) {
        FlowFile flowFile = null;
        try {
            flowFile = session.create();
            Map<String, String> attributes = new HashMap<>();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field("job name", context.getProperty(JOB_NAME).getValue()));
            fields.add(new Field("job id", id));
            fields.add(new Field("job state", state));
            Message message = new Message();
            message.setFallback("Job with id " + id + " is in state " + state);
            message.setTitle("Job report details:");
            switch (state) {
                case "JOB_DRY_RUN":
                    message.setPretext(":warning: Job dry run report");
                    message.setColor(MessageColor.WARNING.color);
                    break;
                case "JOB_STATE_RUNNING":
                case "JOB_STATE_PENDING":
                    message.setPretext(":warning: Job in action report");
                    message.setColor(MessageColor.WARNING.color);
                    break;
                case "JOB_STATE_DONE":
                    message.setPretext(":white_check_mark: Job done report");
                    message.setColor(MessageColor.GOOD.color);
                    break;
                default:
                    message.setPretext(":exclamation: Job failed report");
                    message.setColor(MessageColor.DANGER.color);
                    break;

            }
            message.setFields(fields);
            try {
                attributes.put("attachments", (new ObjectMapper()).writeValueAsString(message));
                if (attributes.size() > 0) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }
                session.transfer(flowFile, REL_NOTIFY);
            } catch (JsonProcessingException e) {
                getLogger().error("Can't create notification!", e);
                session.remove(flowFile);
            }
        } catch (Exception e) {
            getLogger().error("Can't create notification!", e);
            if (flowFile != null) {
                session.remove(flowFile);
            }
        }
    }

    private Job wetRun(ProcessContext context, ProcessSession session, Dataflow dataflowService, LaunchTemplateResponse launchResponse) throws IOException {
        Job content = launchResponse.getJob();
        String state = content.getCurrentState();
        while (true) {
            if (state == null || state.equals("JOB_STATE_RUNNING") || state.equals("JOB_STATE_PENDING")) {
                context.yield();
            } else if (state.equals("JOB_STATE_DONE")) {
                getLogger().info("The job has done");
                notifyJobState(context, session, content.getId(), state);
                break;
            } else if (state.equals("JOB_STATE_UNKNOWN")) {
                notifyJobState(context, session, content.getId(), state);
                throw new ProcessException("You have no access to the job state." +
                        " Job id = " + content.getId() +
                        " job name = " + context.getProperty(JOB_NAME).getValue() +
                        " job template = " + context.getProperty(GCS_PATH).getValue()
                );
            } else {
                notifyJobState(context, session, content.getId(), state);
                throw new ProcessException("Failed to done a job." +
                        " Job id = " + content.getId() +
                        " job name = " + context.getProperty(JOB_NAME).getValue() +
                        " job template = " + context.getProperty(GCS_PATH).getValue() +
                        " job state = " + state
                );
            }
            Dataflow.Projects.Locations.Jobs.Get request = dataflowService.projects().locations().jobs().get(
                    context.getProperty(PROJECT_ID).getValue(),
                    "europe-west1",
                    content.getId()
            );
            Job response = request.execute();
            if (response.getCurrentState().equals("JOB_STATE_RUNNING") && (state == null || !state.equals("JOB_STATE_RUNNING"))) {
                notifyJobState(context, session, response.getId(), response.getCurrentState());
            }
            state = response.getCurrentState();
            getLogger().info("The job is in state: " + state);
        }
        return content;
    }


    private FlowFile buildFlowFile(
            ProcessContext context,
            ProcessSession session,
            Map<String, String> parameters,
            FlowFile flowFile
    ) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(JOB_NAME.getName(), context.getProperty(JOB_NAME).getValue());
        attributes.put(VALIDATE_ONLY.getName(), context.getProperty(VALIDATE_ONLY).getValue());
        attributes.put(GCS_PATH.getName(), context.getProperty(GCS_PATH).getValue());
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            attributes.put(entry.getKey(), entry.getValue());
        }
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

}
