package com.fluiggers.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.logging.Logger;

import com.fluiggers.dto.SuccessesAndErrorsDto;
import com.fluiggers.dto.WorkflowEventDto;
import com.fluiggers.dto.WorkflowProcessDto;
import com.fluiggers.exception.WorkflowNotFoundedException;
import com.fluiggers.service.WorkflowService;

public class WorkflowRepository extends BaseRepository {
    protected final Logger log = Logger.getLogger(getClass());

    public int findMaxVersion(long tenantId, String processId) throws SQLException, Exception {
        InitialContext ic = null;
        int version = 0;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            "SELECT MAX(NUM_VERS) AS MAX_VERSION "
                                    + "FROM VERS_DEF_PROCES "
                                    + "WHERE COD_EMPRESA = ? AND COD_DEF_PROCES = ?")) {
                stmt.setLong(1, tenantId);
                stmt.setString(2, processId);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        version = rs.getInt("MAX_VERSION");
                    }
                }
            }
        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }

        if (version == 0) {
            throw new WorkflowNotFoundedException();
        }

        return version;
    }

    public Set<String> getEventsFromWorkflow(
            long tenantId,
            String processId,
            int version) {
        Set<String> events = new HashSet<>();

        InitialContext ic = null;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            "SELECT COD_EVENT AS EVENT "
                                    + "FROM event_proces "
                                    + "WHERE COD_EMPRESA = ? AND COD_DEF_PROCES = ? AND NUM_VERS = ?")) {
                stmt.setLong(1, tenantId);
                stmt.setString(2, processId);
                stmt.setInt(3, version);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        events.add(rs.getString("EVENT"));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }

        return events;
    }

    public SuccessesAndErrorsDto updateEvents(
            long tenantId,
            String processId,
            int version,
            List<WorkflowEventDto> events) {
        var result = new SuccessesAndErrorsDto();

        InitialContext ic = null;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            "UPDATE event_proces "
                                    + "SET DSL_EVENT = ? "
                                    + "WHERE "
                                    + "COD_EMPRESA = ? "
                                    + "AND COD_DEF_PROCES = ? "
                                    + "AND COD_EVENT = ? "
                                    + "AND NUM_VERS = ? ")) {
                conn.setAutoCommit(false);

                stmt.setLong(2, tenantId);
                stmt.setString(3, processId);
                stmt.setInt(5, version);

                for (WorkflowEventDto event : events) {
                    stmt.setString(1, event.getContents());
                    stmt.setString(4, event.getName());

                    stmt.addBatch();
                }

                int[] updatedRows = stmt.executeBatch();
                conn.commit();

                for (var i = 0; i < updatedRows.length; ++i) {
                    if (updatedRows[i] == 0) {
                        result.addError(events.get(i).getName() + " não foi atualizado.");
                        continue;
                    }

                    result.addSuccess(events.get(i).getName() + " foi atualizado.");
                }
            }
        } catch (Exception e) {
            log.error(e);

            for (WorkflowEventDto event : events) {
                result.addError(event.getName() + " não foi atualizado.");
            }

        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }

        return result;
    }

    public SuccessesAndErrorsDto createEvents(
            long tenantId,
            String processId,
            int version,
            String userCode,
            List<WorkflowEventDto> events) {
        var result = new SuccessesAndErrorsDto();

        InitialContext ic = null;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();

                    PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO event_proces "
                                    + "(COD_EMPRESA, COD_DEF_PROCES, NUM_VERS, SCRIPT_VERSION, AUTHOR_ID, CREATE_DATE, COD_EVENT, DSL_EVENT) "
                                    + "VALUES  (?, ?, ?, ?, ?, ?, ?, ?)")) {
                conn.setAutoCommit(false);

                stmt.setLong(1, tenantId);
                stmt.setString(2, processId);
                stmt.setInt(3, version);
                stmt.setInt(4, 0);
                stmt.setString(5, userCode);
                stmt.setTimestamp(6, Timestamp.from(Instant.now()));

                for (WorkflowEventDto event : events) {
                    stmt.setString(7, event.getName());
                    stmt.setString(8, event.getContents());

                    stmt.addBatch();
                }

                int[] createdRows = stmt.executeBatch();
                conn.commit();

                for (var i = 0; i < createdRows.length; ++i) {
                    if (createdRows[i] == 0) {
                        result.addError(events.get(i).getName() + " não foi criado.");
                        continue;
                    }

                    result.addSuccess(events.get(i).getName() + " foi criado.");
                }
            }
        } catch (Exception e) {
            log.error(e);

            for (WorkflowEventDto event : events) {
                result.addError(event.getName() + " não foi criado.");
            }

        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }

        return result;
    }

    public List<WorkflowProcessDto> findProcesses(long tenantId) throws Exception {
        List<WorkflowProcessDto> result = new java.util.ArrayList<>();

        InitialContext ic = null;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            "SELECT COD_DEF_PROCES, MAX(NUM_VERS) AS VERSION " +
                                    "FROM VERS_DEF_PROCES " +
                                    "WHERE COD_EMPRESA = ? " +
                                    "GROUP BY COD_DEF_PROCES " +
                                    "ORDER BY COD_DEF_PROCES")) {

                stmt.setLong(1, tenantId);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        WorkflowProcessDto dto = new WorkflowProcessDto();

                        dto.setProcessId(rs.getString("COD_DEF_PROCES"));
                        dto.setVersion(rs.getInt("VERSION"));
                        dto.setDescription(dto.getProcessId());

                        dto.setActive(true);

                        result.add(dto);
                    }
                }
            }

        } catch (Exception e) {
            log.error(e);
            throw e;
        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }

        return result;
    }

    public Object importProcess(long tenantId, String processId, int version) throws Exception {

        InitialContext ic = null;

        try {
            ic = new InitialContext();
            DataSource ds = (DataSource) ic.lookup(DB_DATASOURCE_NAME);

            try (
                    Connection conn = ds.getConnection();
                    PreparedStatement stmt = conn.prepareStatement(
                            "SELECT " +
                                    "dp.cod_empresa, " +
                                    "dp.cod_def_proces AS process_id, " +
                                    "dp.des_def_proces AS description, " +
                                    "vdp.num_vers AS version, " +
                                    "vdp.clb_img_proces AS process_xml, " +
                                    "ep.cod_event AS event_name, " +
                                    "ep.dsl_event AS event_script " +
                                    "FROM def_proces dp " +
                                    "INNER JOIN vers_def_proces vdp " +
                                    "   ON vdp.cod_empresa = dp.cod_empresa " +
                                    "  AND vdp.cod_def_proces = dp.cod_def_proces " +
                                    "LEFT JOIN event_proces ep " +
                                    "   ON ep.cod_empresa = dp.cod_empresa " +
                                    "  AND ep.cod_def_proces = dp.cod_def_proces " +
                                    "  AND ep.num_vers = vdp.num_vers " +
                                    "WHERE dp.cod_empresa = ? " +
                                    "  AND dp.cod_def_proces = ? " +
                                    "  AND vdp.num_vers = ?")) {

                stmt.setLong(1, tenantId);
                stmt.setString(2, processId);
                stmt.setInt(3, version);

                try (ResultSet rs = stmt.executeQuery()) {

                    Map<String, Object> result = new HashMap<>();
                    List<Map<String, Object>> events = new ArrayList<>();
                    Set<String> eventKeys = new HashSet<>();

                    while (rs.next()) {
                        if (!result.containsKey("definition")) {
                            result.put("processId", rs.getString("process_id"));
                            result.put("description", rs.getString("description"));
                            result.put("version", rs.getInt("version"));
                            result.put("definition", rs.getString("process_xml"));
                        }

                        String eventName = rs.getString("event_name");
                        if (eventName != null && !eventName.trim().isEmpty()) {
                            if (!eventKeys.contains(eventName)) {
                                eventKeys.add(eventName);

                                Map<String, Object> ev = new HashMap<>();
                                ev.put("name", eventName);
                                ev.put("script", rs.getString("event_script"));
                                events.add(ev);
                            }
                        }
                    }

                    if (result.isEmpty()) {
                        throw new Exception("Processo não encontrado.");
                    }

                    result.put("events", events);
                    return result;
                }
            }

        } catch (Exception e) {
            log.error(e);
            throw e;
        } finally {
            if (ic != null) {
                try {
                    ic.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

}
