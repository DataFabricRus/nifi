package org.apache.nifi.processors.gcp.dataflow;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.TimeUnit;


@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
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


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .add(PROJECT_ID)
                .add(JOB_NAME)
                .add(GCS_PATH)
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

    private static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google Cloud Dataflow launch operation.")
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
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = null;
        try {
            // Authentication is provided by gcloud tool when running locally
            // and by built-in service accounts when running on GAE, GCE or GKE.
            GoogleCredential credential = GoogleCredential.getApplicationDefault();

            // The createScopedRequired method returns true when running on GAE or a local developer
            // machine. In that case, the desired scopes must be passed in manually. When the code is
            // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
            // See https://developers.google.com/identity/protocols/application-default-credentials for more information.
            if (credential.createScopedRequired()) {
                credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

            com.google.auth.oauth2.GoogleCredentials googleCredentials = com.google.auth.oauth2.GoogleCredentials.getApplicationDefault();


            Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
                    .setApplicationName("Google Cloud Platform Sample")
                    .build();

            // Add your code to assign values to parameters for the 'create' method:
            // * The project which owns the job.
            String projectId = context.getProperty(PROJECT_ID).getValue();

            CreateJobFromTemplateRequest createJobFromTemplateRequest = new CreateJobFromTemplateRequest();
            createJobFromTemplateRequest.setJobName(context.getProperty(JOB_NAME).getValue());
            createJobFromTemplateRequest.setGcsPath(context.getProperty(GCS_PATH).getValue());


            Map<String, String> parameters = new HashMap<>();
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                if (entry.getKey().isDynamic()) {
                    final String value = context.getProperty(entry.getKey()).getValue();
                    parameters.put(entry.getKey().getName(), value);
                }
            }
            createJobFromTemplateRequest.setParameters(parameters);

            Dataflow.Projects.Templates.Create create = dataflowService.projects().templates().create(projectId, createJobFromTemplateRequest);

            final long startNanos = System.nanoTime();
            Job content = create.execute();
            String state = content.getCurrentState();
            while (true) {
                if (state == null || state.equals("JOB_STATE_RUNNING") || state.equals("JOB_STATE_PENDING")) {
                    context.yield();
                } else if (state.equals("JOB_STATE_DONE")) {
                    getLogger().info("The job has done");
                    break;
                } else if (state.equals("JOB_STATE_UNKNOWN")) {
                    throw new ProcessException("You have no access to the job state, job state is " + state);
                } else {
                    throw new ProcessException("Failed to done a job, the job state is " + state);
                }
                Dataflow.Projects.Jobs.Get request = dataflowService.projects().jobs().get(projectId, content.getId());
                Job response = request.execute();
                state = response.getCurrentState();
                getLogger().info("The job is in state: " + state);
            }

            flowFile = session.create();
            Map<String, String> attributes = new HashMap<>();
            attributes.put("filename", content.getId());
            attributes.put("projectId", projectId);
            attributes.put("jobId", content.getId());
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                attributes.put(entry.getKey(), entry.getValue());
            }
            if (attributes.size() > 0) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().create(flowFile, "It takes "
                    + millis
                    + " to perform the job that is reported with that flow file");
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("the report about the job is presented with flow file {}", new Object[]{flowFile});
            session.commit();

        } catch (final Exception e) {
            getLogger().error("Failed to create flow file reporting the job due to{}", e);
            if (flowFile != null) {
                session.remove(flowFile);
            }
        }

    }


}
