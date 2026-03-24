package com.fluiggers.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.activation.DataHandler;
import java.io.InputStream;

import com.fluiggers.dto.SuccessesAndErrorsDto;
import com.fluiggers.dto.WorkflowEventDto;
import com.fluiggers.dto.WorkflowProcessDto;
import com.fluiggers.dto.WorkflowUpdatedEventsDto;
import com.fluiggers.repository.WorkflowRepository;

public class WorkflowService {
    public int getMaxVersion(long tenantId, String processId) throws Exception {
        var repository = new WorkflowRepository();
        return repository.findMaxVersion(tenantId, processId);
    }

    public WorkflowUpdatedEventsDto updateEvents(
            long tenantId,
            String processId,
            int version,
            String userCode,
            List<WorkflowEventDto> events) {
        var repository = new WorkflowRepository();

        Set<String> workflowEvents = repository.getEventsFromWorkflow(tenantId, processId, version);

        List<WorkflowEventDto> eventsToUpdate = new ArrayList<>();
        List<WorkflowEventDto> eventsToCreate = new ArrayList<>();

        for (WorkflowEventDto event : events) {
            if (workflowEvents.contains(event.getName())) {
                eventsToUpdate.add(event);
                continue;
            }

            eventsToCreate.add(event);
        }

        SuccessesAndErrorsDto updatedEvents = repository.updateEvents(tenantId, processId, version, eventsToUpdate);
        SuccessesAndErrorsDto createdEvents = repository.createEvents(tenantId, processId, version, userCode,
                eventsToCreate);

        var result = new WorkflowUpdatedEventsDto();
        result.setProcessId(processId);
        result.setVersion(version);
        result.setHasError(updatedEvents.getErrors().size() != 0 || createdEvents.getErrors().size() != 0);
        result.setTotalProcessed(
                updatedEvents.getSuccesses().size()
                        + updatedEvents.getErrors().size()
                        + createdEvents.getSuccesses().size()
                        + createdEvents.getErrors().size());

        result.setErrors(
                Stream
                        .concat(updatedEvents.getErrors().stream(), createdEvents.getErrors().stream())
                        .collect(Collectors.toList()));

        result.setSuccesses(
                Stream
                        .concat(updatedEvents.getSuccesses().stream(), createdEvents.getSuccesses().stream())
                        .collect(Collectors.toList()));

        return result;
    }

    public List<WorkflowProcessDto> getProcesses(long tenantId) throws Exception {
        var repository = new WorkflowRepository();
        return repository.findProcesses(tenantId);
    }

    public Object exportProcess(long tenantId, String processId, int version) throws Exception {
        var repository = new WorkflowRepository();
        return repository.exportProcess(tenantId, processId, version);
    }
}
