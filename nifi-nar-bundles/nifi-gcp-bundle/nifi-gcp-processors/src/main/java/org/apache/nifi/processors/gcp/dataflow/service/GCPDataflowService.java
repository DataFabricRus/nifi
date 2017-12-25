package org.apache.nifi.processors.gcp.dataflow.service;

import com.google.api.services.dataflow.Dataflow;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;


@Tags({"gcp", "datflow", "service", "provider"})
@CapabilityDescription("Provides dataflow service.")
public interface GCPDataflowService extends ControllerService {
    Dataflow getDataflowService();
}
