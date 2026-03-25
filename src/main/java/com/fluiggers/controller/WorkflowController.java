package com.fluiggers.controller;

import java.util.List;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fluiggers.dto.WorkflowEventDto;
import com.fluiggers.dto.WorkflowProcessDto;
import com.fluiggers.dto.WorkflowUpdatedEventsDto;
import com.fluiggers.exception.WorkflowNotFoundedException;
import com.fluiggers.service.WorkflowService;

@Path("workflows")
public class WorkflowController extends BaseController {

    @GET
    @Path("/{processId}/version")
    @Produces(MediaType.APPLICATION_JSON)
    public int maxVersion(@PathParam("processId") String processId) {

        assertUserAccess();

        if (processId.isBlank()) {
            throw new BadRequestException("Necessário informar o processId");
        }

        try {
            var service = new WorkflowService();
            return service.getMaxVersion(securityService.getCurrentTenantId(), processId);
        } catch (WorkflowNotFoundedException e) {
            throw new NotFoundException(e.getMessage());
        } catch (Exception e) {
            log.error(e);
            throw new InternalServerErrorException("Consulte o Log do Fluig para mais informações.");
        }
    }

    @PUT
    @Path("/{processId}/{version:[1-9][0-9]*}/events")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public WorkflowUpdatedEventsDto updateWorkflowEvents(
            @PathParam("processId") String processId,
            @PathParam("version") int version,
            List<WorkflowEventDto> events) {

        assertUserAccess();

        if (processId.isBlank()) {
            throw new BadRequestException("Necessário informar o processId");
        }

        for (WorkflowEventDto event : events) {
            if (event.getName().isBlank() || event.getContents().isBlank()) {
                throw new BadRequestException("Obrigatório que todos os eventos possuam `name` e `contents`");
            }

            event.setName(event.getName().trim());
        }

        var service = new WorkflowService();
        long tenantId;
        String userCode;
        int maxVersion;

        try {
            tenantId = securityService.getCurrentTenantId();
            userCode = userService.getCurrent().getCode();
            maxVersion = service.getMaxVersion(tenantId, processId);
        } catch (WorkflowNotFoundedException e) {
            throw new NotFoundException(e.getMessage());
        } catch (Exception e) {
            log.error(e);
            throw new InternalServerErrorException("Consulte o Log do Fluig para mais informações.");
        }

        if (version > maxVersion) {
            throw new BadRequestException("A versão indicada deve ser menor ou igual a " + maxVersion);
        }

        try {
            return service.updateEvents(tenantId, processId, version, userCode, events);
        } catch (Exception e) {
            log.error(e);
            throw new InternalServerErrorException("Consulte o Log do Fluig para mais informações.");
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<WorkflowProcessDto> listProcesses() {

        assertUserAccess();

        try {
            var service = new WorkflowService();
            return service.getProcesses(securityService.getCurrentTenantId());
        } catch (Exception e) {
            log.error(e);
            throw new InternalServerErrorException("Consulte o Log do Fluig para mais informações.");
        }
    }

    @GET
    @Path("/{processId}/{version}/import")
    @Produces(MediaType.APPLICATION_JSON)
    public Response exportProcess(
            @PathParam("processId") String processId,
            @PathParam("version") int version) {

        assertUserAccess();

        try {
            var service = new WorkflowService();

            Object result = service.exportProcess(
                    securityService.getCurrentTenantId(),
                    processId,
                    version);

            return Response.ok(result).build();

        } catch (Exception e) {
            log.error(e);
            throw new InternalServerErrorException("Erro ao importar processo.");
        }
    }
}
