/*
 * pg_partman_bgw.c
 *
 * A background worker process for the pg_partman extension to allow
 * partition maintenance to be scheduled and run within the database
 * itself without required a third-party scheduler (ex. cron)
 *
 */

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "utils/date.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

void    _PG_init(void);
void    pg_partman_bgw_main(Datum);
void    pg_partman_bgw_run_maint(Datum);
void    pg_partman_run_maintenance_c(char*);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int pg_partman_bgw_interval = 3600; // Default hourly
static char *pg_partman_bgw_role = "postgres"; // Default to postgres role
static char *pg_partman_bgw_analyze = "on";
static char *pg_partman_bgw_jobmon = "on";
static char *pg_partman_bgw_dbname = NULL;


/*
 * Signal handler for SIGTERM
 *      Set a flag to let the main loop to terminate, and set our latch to wake
 *      it up.
 */
static void
pg_partman_bgw_sigterm(SIGNAL_ARGS)
{
    int         save_errno = errno;

    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *      Set a flag to tell the main loop to reread the config file, and set
 *      our latch to wake it up.
 */
static void pg_partman_bgw_sighup(SIGNAL_ARGS) {
    int         save_errno = errno;

    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
}


/*
 * Entrypoint of this module.
 */
void
_PG_init(void)
{
    BackgroundWorker worker;

    DefineCustomIntVariable("pg_partman_bgw.interval",
                            "How often run_maintenance() is called (in seconds).",
                            NULL,
                            &pg_partman_bgw_interval,
                            3600,
                            1,
                            INT_MAX,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_partman_bgw.analyze",
                            "Whether to run an analyze on a partition set whenever a new partition is created during run_maintenance(). Set to 'on' to send TRUE (default). Set to 'off' to send FALSE.",
                            NULL,
                            &pg_partman_bgw_analyze,
                            "on",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_partman_bgw.dbname",
                            "CSV list of specific databases in the cluster to run pg_partman BGW on. This setting is not required and when not set, pg_partman will dynamically figure out which ones to run on. If set, forces the BGW to only run on these specific databases and never any others. Recommend leaving this unset unless necessary.",
                            NULL,
                            &pg_partman_bgw_dbname,
                            NULL,
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_partman_bgw.jobmon",
                            "Whether to log run_maintenance() calls to pg_jobmon if it is installed. Set to 'on' to send TRUE (default). Set to 'off' to send FALSE.",
                            NULL,
                            &pg_partman_bgw_jobmon,
                            "on",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_partman_bgw.role",
                               "Role to be used by BGW. Must have execute permissions on run_maintenance()",
                               NULL,
                               &pg_partman_bgw_role,
                               "postgres",
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);

/* Kept as comment for reference for future development
    DefineCustomStringVariable("pg_partman_bgw.maintenance_db",
                            "The BGW requires connecting to a local database for reading system catalogs. By default it uses template1. You can change that with this setting if needed.",
                            NULL,
                            &pg_partman_bgw_maint_db,
                            "template1",
                            PGC_SIGHUP,
                            0,
                            NULL,
                            NULL,
                            NULL);
*/

    if (!process_shared_preload_libraries_in_progress)
        return;

    // Start BGW when database starts
    sprintf(worker.bgw_name, "pg_partman master background worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
        BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_main = pg_partman_bgw_main;
    worker.bgw_main_arg = CStringGetDatum(pg_partman_bgw_dbname);
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);

}


void pg_partman_bgw_main(Datum main_arg) {
    StringInfoData buf;

    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, pg_partman_bgw_sighup);
    pqsignal(SIGTERM, pg_partman_bgw_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

/* Keep for when master requires persistent connection
   elog(LOG, "%s master process initialized with role %s on database %s"
            , MyBgworkerEntry->bgw_name
            , pg_partman_bgw_role
            , pg_partman_bgw_dbname);
*/
   elog(LOG, "%s master process initialized with role %s"
            , MyBgworkerEntry->bgw_name
            , pg_partman_bgw_role);

    initStringInfo(&buf);

    /*
     * Main loop: do this until the SIGTERM handler tells us to terminate
     */
    while (!got_sigterm) {
        BackgroundWorker        worker;
        BackgroundWorkerHandle  *handle;
        BgwHandleStatus         status;
        char                    *rawstring;
        int                     dbcounter;
        int                     rc;
        List                    *elemlist;
        ListCell                *l;
        pid_t                   pid;

        /*
         * Background workers mustn't call usleep() or any direct equivalent:
         * instead, they may wait on their process latch, which sleeps as
         * necessary, but is awakened if postmaster dies.  That way the
         * background process goes away immediately in an emergency.
         */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       pg_partman_bgw_interval * 1000L);
        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }

        /* In case of a SIGHUP, just reload the configuration. */
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* In case of a SIGTERM in middle of loop, stop all further processing and return from BGW function to allow it to exit cleanly. */
        if (got_sigterm) {
            elog(LOG, "pg_partman master BGW received SIGTERM. Shutting down.");
            return;
        }

        // Use method of shared_preload_libraries to split the pg_partman_bgw_dbname string found in src/backend/utils/init/miscinit.c 
        // Need a modifiable copy of string 
        if (pg_partman_bgw_dbname != NULL) {
            rawstring = pstrdup(pg_partman_bgw_dbname);
            // Parse string into list of identifiers 
            if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
                // syntax error in list 
                pfree(rawstring);
                list_free(elemlist);
                ereport(LOG,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("invalid list syntax in parameter \"pg_partman_bgw.dbname\" in postgresql.conf")));
                return;
            }
            dbcounter = 0;
            foreach(l, elemlist) {

                char *dbname = (char *) lfirst(l);

                elog(DEBUG1, "Dynamic bgw launch begun for %s (%d)", dbname, dbcounter);
                worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                    BGWORKER_BACKEND_DATABASE_CONNECTION;
                worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
                worker.bgw_restart_time = BGW_NEVER_RESTART;
                worker.bgw_main = NULL;
                sprintf(worker.bgw_library_name, "pg_partman_bgw");
                sprintf(worker.bgw_function_name, "pg_partman_bgw_run_maint");
                sprintf(worker.bgw_name, "pg_partman dynamic background worker (dbname=%s)", dbname);
                worker.bgw_main_arg = Int32GetDatum(dbcounter);
                worker.bgw_notify_pid = MyProcPid;

                dbcounter++;

                if (!RegisterDynamicBackgroundWorker(&worker, &handle))
                    elog(FATAL, "Unable to register dynamic background worker for pg_partman");
                    continue;

                status = WaitForBackgroundWorkerStartup(handle, &pid);

                if (status == BGWH_STOPPED)
                    ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                             errmsg("Could not start dynamic pg_partman background process"),
                           errhint("More details may be available in the server log.")));
                if (status == BGWH_POSTMASTER_DIED)
                    ereport(ERROR,
                            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                          errmsg("Cannot start dynamic pg_partman background processes without postmaster"),
                             errhint("Kill all remaining database processes and restart the database.")));
                Assert(status == BGWH_STARTED);
            }

            pfree(rawstring);
            list_free(elemlist);
        } else { // pg_partman_bgw_dbname if null
            elog(DEBUG1, "pg_partman_bgw.dbname GUC is NULL. Nothing to do in main loop.");
        }
        continue;

    } // end sigterm while

} // end main

/*
 * Unable to pass the database name as a string argument (not sure why yet)
 * Instead, the GUC is parsed both in the main function and below and a counter integer 
 *  is passed to determine which database the BGW will run in.
 */
void pg_partman_bgw_run_maint(Datum arg) {

//TODO REMOVE    char                *analyze;
    char                *dbname = "template1";
//TODO REMOVE    char                *jobmon;
//TODO REMOVE    char                *partman_schema;
    char                *rawstring;
    int                 dbcounter;
    int                 db_main_counter = DatumGetInt32(arg);
    List                *elemlist;
    ListCell            *l;
    int                 ret;
    StringInfoData      buf;

    /* Establish signal handlers before unblocking signals. */
    pqsignal(SIGHUP, pg_partman_bgw_sighup);
    pqsignal(SIGTERM, pg_partman_bgw_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    elog(DEBUG1, "Before parsing dbname GUC in dynamic main func: %s", pg_partman_bgw_dbname);
    rawstring = pstrdup(pg_partman_bgw_dbname);
    elog(DEBUG1, "GUC rawstring copy: %s", rawstring);
    // Parse string into list of identifiers 
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        // syntax error in list 
        pfree(rawstring);
        list_free(elemlist);
        ereport(LOG,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("invalid list syntax in parameter \"pg_partman_bgw.dbname\" in postgresql.conf")));
        return;
    }
    dbcounter = 0;
    foreach(l, elemlist) {
        elog(DEBUG1, "Entered foreach loop: name (%s), db_main_counter (%d), dbcounter (%d)", (char *) lfirst(l), db_main_counter, dbcounter);
        if (db_main_counter == dbcounter) {
            dbname = (char *) lfirst(l);
            elog(DEBUG1, "Parsing list: %s (%d)", dbname, dbcounter);
        }
        dbcounter++;
    }
    if (strcmp(dbname, "template1") == 0) {
        elog(DEBUG1, "Default database name found in dbname local variable (\"template1\").");
    }

    elog(DEBUG1, "Before run_maint initialize connection for db %s", dbname);
    BackgroundWorkerInitializeConnection(dbname, pg_partman_bgw_role);
    elog(DEBUG1, "After run_maint initialize connection for db %s", dbname);

    initStringInfo(&buf);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_appname("pg_partman dynamic background worker");

    // First determine if pg_partman is even installed in this database
    appendStringInfo(&buf, "SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'pg_partman'");
    pgstat_report_activity(STATE_RUNNING, buf.data);
    elog(DEBUG1, "Checking if pg_partman extension is installed in database: %s" , dbname);
    ret = SPI_execute(buf.data, true, 1);
    if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Cannot determine if pg_partman is installed in database %s: error code %d", dbname, ret);
    }
    if (SPI_processed <= 0) {
        elog(DEBUG1, "pg_partman not installed in database %s. Nothing to do so dynamic worker exiting gracefully.", dbname);
        // Nothing left to do. Return end the run of BGW function.
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity(STATE_IDLE, NULL);

        pfree(rawstring);
        list_free(elemlist);

        return;
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);

    // If so then actually log that it's started for that database. 
    elog(LOG, "%s dynamic background worker initialized with role %s on database %s"
            , MyBgworkerEntry->bgw_name
            , pg_partman_bgw_role
            , dbname);

    resetStringInfo(&buf);

    pg_partman_run_maintenance_c(dbname);

/* TODO REMOVE
    appendStringInfo(&buf, "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid WHERE extname = 'pg_partman'");
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 1);

    if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Cannot determine which schema pg_partman has been installed to: error code %d", ret);
    }

    if (SPI_processed > 0) {
        bool isnull;

        partman_schema = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[0]
                , SPI_tuptable->tupdesc
                , 1
                , &isnull));

        if (isnull)
            elog(FATAL, "Query to determine pg_partman schema returned NULL.");

    } else {
        elog(FATAL, "Query to determine pg_partman schema returned zero rows.");
    }

    resetStringInfo(&buf);
    if (strcmp(pg_partman_bgw_analyze, "on") == 0) {
        analyze = "true";
    } else {
        analyze = "false";
    }
    if (strcmp(pg_partman_bgw_jobmon, "on") == 0) {
        jobmon = "true";
    } else {
        jobmon = "false";
    }
    appendStringInfo(&buf, "SELECT %s.run_maintenance(p_analyze := %s, p_jobmon := %s)", partman_schema, analyze, jobmon);

    pgstat_report_activity(STATE_RUNNING, buf.data);

    ret = SPI_execute(buf.data, false, 0);

    if (ret != SPI_OK_SELECT)
        elog(FATAL, "Cannot call pg_partman run_maintenance() function: error code %d", ret);

    elog(LOG, "%s: %s called by role %s on database %s"
            , MyBgworkerEntry->bgw_name
            , buf.data
            , pg_partman_bgw_role
            , dbname);

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);
*/

    elog(DEBUG1, "pg_partman dynamic BGW shutting down gracefully for database %s.", dbname);

    pfree(rawstring);
    list_free(elemlist);

    return;
}

void pg_partman_run_maintenance_c(char *dbname) {
/* Add jobmon stuff in later if possible */

//    char                *analyze;
//    char                *jobmon;
    char                *partman_schema;
    char                *parent_table;
    int                 ret;
    int                 i;
    StringInfoData      buf;
    
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid WHERE extname = 'pg_partman'");
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 1);

    if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Cannot determine which schema pg_partman has been installed to: error code %d", ret);
    }

    if (SPI_processed > 0) {

        partman_schema = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
/* Left here as example for when needing to check for null
 *          partman_schema = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[0]
                , SPI_tuptable->tupdesc
                , 1
                , &isnull));
*/
    } else {
        elog(FATAL, "Query to determine pg_partman schema returned zero rows.");
    }
/* TODO
    if (strcmp(pg_partman_bgw_analyze, "on") == 0) {
        analyze = "true";
    } else {
        analyze = "false";
    }
    if (strcmp(pg_partman_bgw_jobmon, "on") == 0) {
        jobmon = "true";
    } else {
        jobmon = "false";
    }
   */ 
    resetStringInfo(&buf);
    appendStringInfo(&buf, "SELECT parent_table, partition_type, partition_interval, control, premake, datetime_string, undo_in_progress, sub_partition_set_full, epoch, infinite_time_partitions FROM %s.part_config WHERE sub_partition_set_full = false AND use_run_maintenance = true", partman_schema);
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Cannot get list of partitions to run maintenance on: error code %d", ret);
    }
    //TODO REMOVE
    elog(LOG, "SPI_Processed: %d", SPI_processed);
    if (SPI_processed > 0) {
        int                 premake;
        int                 parent_table_count;
        char                *partition_type;
        char                *partition_interval;
        char                *control;
        char                *datetime_string;
        char                *parent_schemaname;
        char                *parent_tablename;
        char                *last_partition;
        bool                epoch;
        bool                infinite_time_partitions;
        bool                datetime_isnull;
        bool                sub_partition_set_full;
        bool                undo_in_progress;
        bool                isnull; // generic one for columns I don't care because they can't be null
        SPITupleTable       *parent_table_rows;

        parent_table_rows = SPI_tuptable;
        parent_table_count = SPI_processed;

        for (i=0; i < parent_table_count; i++) {
            parent_table = SPI_getvalue(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "parent_table")); 
            partition_type = SPI_getvalue(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "partition_type")); 
            partition_interval = SPI_getvalue(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "partition_interval")); 
            control = SPI_getvalue(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "control")); 
            premake = DatumGetInt32(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "premake"), &isnull)); 
            datetime_string = DatumGetCString(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "datetime_string"), &datetime_isnull)); 
            undo_in_progress = DatumGetBool(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "undo_in_progress"), &isnull)); 
            sub_partition_set_full = DatumGetBool(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "sub_partition_set_full"), &isnull)); 
            epoch = DatumGetBool(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "epoch"), &isnull)); 
            infinite_time_partitions = DatumGetBool(SPI_getbinval(parent_table_rows->vals[i], parent_table_rows->tupdesc, SPI_fnumber(parent_table_rows->tupdesc, "infinite_time_partitions"), &isnull)); 

            elog(LOG, "Just checking that this thing is working. Loop: %d, parent_table: %s, partition_type: %s, partition_interval: %s, control: %s, premake: %d, datetime_string: %s, undo_in_progress: %d, sub_partition_set_full: %d, epoch: %d, infinite_time_partitions: %d", i, parent_table, partition_type, partition_interval, control, premake, datetime_string, undo_in_progress, sub_partition_set_full, epoch, infinite_time_partitions);

            if(undo_in_progress != 0) {
                continue;
            } 

            resetStringInfo(&buf);
            /* Check for consistent data in part_config_sub table. Was unable to get this working properly as either a constraint or trigger. 
             * Would either delay raising an error until the next write (which I cannot predict) or disallow future edits to update a sub-partition set's configuration.
             * This way at least provides a consistent way to check that I know will run. If anyone can get a working constraint/trigger, please help!
             * Don't have to worry about this in the serial trigger maintenance since subpartitioning requires run_maintenance(). */
            appendStringInfo(&buf, "SELECT sub_parent FROM %s.part_config_sub WHERE sub_parent = '%s'", partman_schema, parent_table);
            pgstat_report_activity(STATE_RUNNING, buf.data);
            ret = SPI_execute(buf.data, true, 1);
            if (SPI_processed > 0) {
                char    *sub_parent;
                int     check_subpart;

                sub_parent = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
                resetStringInfo(&buf);
                appendStringInfo(&buf, "SELECT count(*) FROM %s.check_subpart_sameconfig('%s')", partman_schema, parent_table);
                pgstat_report_activity(STATE_RUNNING, buf.data);
                elog(DEBUG1, "Checking for consistent sub-partition configurations");
                ret = SPI_execute(buf.data, true, 1);
                check_subpart = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
                if(check_subpart > 1) {
                    elog(FATAL, "Inconsistent data in part_config_sub table. Sub-partition tables that are themselves sub-partitions cannot have differing configuration values among their siblings.\nRun this query: \"SELECT * FROM @extschema@.check_subpart_sameconfig(''%s'');\" This should only return a single row or nothing.\nIf multiple rows are returned, results are all children of the given parent. Update the differing values to be consistent for your desired values.", sub_parent);
                }
            } // end sub_parent check if

            resetStringInfo(&buf);
            appendStringInfo(&buf, "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = split_part('%s', '.', 1)::name AND tablename = split_part('%s', '.', 2)::name", parent_table, parent_table);
            pgstat_report_activity(STATE_RUNNING, buf.data);
            ret = SPI_execute(buf.data, true, 1);
            if (ret == SPI_OK_SELECT && SPI_processed > 0) {
                parent_schemaname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "schemaname")); 
                parent_tablename = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "tablename")); 


            } else {
                elog(FATAL, "Unable to find parent table in catalog lookup (%s)", parent_table);
            }

            resetStringInfo(&buf);
            appendStringInfo(&buf, "SELECT partition_tablename FROM %s.show_partitions('%s', 'DESC') LIMIT 1", partman_schema, parent_table);
            ret = SPI_execute(buf.data, true, 1);
            last_partition = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, SPI_fnumber(SPI_tuptable->tupdesc, "partition_tablename")); 

            resetStringInfo(&buf);
            if ( strcmp(partition_type, "time") == 0 || strcmp(partition_type, "time-custom") == 0 ) {
                TimeTzADT    *last_partition_timestamp;

                appendStringInfo(&buf, "SELECT child_start_time FROM %s.show_partition_info('%s.%s', '%s', '%s')"
                        , partman_schema
                        , parent_schemaname
                        , last_partition
                        , partition_interval
                        , parent_table);

                ret = SPI_execute(buf.data, true, 1);
                ereport(NOTICE, (errmsg("query=%s", buf.data)));
                ereport(NOTICE, (errmsg("ret=%i -- rows=%ju", ret, SPI_processed)));

                last_partition_timestamp = DatumGetTimeTzADTP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));

                ereport(NOTICE, (errmsg("SPI_result=%d -- ptr=%p", SPI_result, (void*)last_partition_timestamp )));
//                last_partition_timestamp = DatumGetTimeTzADTP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
                
                elog(LOG, "Just seeing if it's time partitioned: %ld", last_partition_timestamp->time);
                //elog(LOG, "Just seeing if it's time partitioned: %s", last_partition );
            // end time if section
            } else if ( strcmp(partition_type, "id") == 0 ) {
                elog(LOG, "Just seeing if it's id partitioned: %s.%s", parent_schemaname, parent_tablename);

            // end id if section
            }


            // TODO May not have to have giant separate IF/ELSE for time vs id maintenance. Just use different Datum retrieval functions
        } // end part_config for loop

    } else {
        elog(DEBUG1, "Found no partitions in part_config to run maintenance on for database: %s.", dbname);
    } 

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);

    //  pfree of SPI_getvalue() variables causing crash. check if this is actually needed?
//    pfree(partman_schema);
}
