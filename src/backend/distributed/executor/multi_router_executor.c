/*
 * multi_router_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * with synchronous connections. The routines utilize the connection cache.
 * Therefore, only a single connection is opened for each worker. Also, router
 * executor does not require a master table and a master query. In other words,
 * the results that are fetched from a single worker is sent to the output console
 * directly. Lastly, router executor can only execute a single task.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"

#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/int8.h"


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

static uint64 shardForTransaction = INVALID_SHARD_ID;
static bool callbackInstalled = false;
static bool txnStarted = false;
static List *participantList = NIL;
static List *failedParticipantList = NIL;

static LOCKMODE CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery);
static void AcquireExecutorShardLock(Task *task, LOCKMODE lockMode);
static uint64 ExecuteDistributedModify(Task *task);
static void ExecuteTransactionEnd(char *sqlCommand);
static void ExecuteSingleShardSelect(QueryDesc *queryDesc, uint64 tupleCount,
									 Task *task, EState *executorState,
									 TupleDesc tupleDescriptor,
									 DestReceiver *destination);
static bool SendQueryInSingleRowMode(PGconn *connection, char *query);
static bool StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
							 Tuplestorestate *tupleStore);
static void RouterTransactionCallback(XactEvent event, void *arg);
static void RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
										 SubTransactionId parentSubid, void *arg);


/*
 * RouterExecutorStart sets up the executor state and queryDesc for router
 * execution.
 */
void
RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task)
{
	LOCKMODE lockMode = NoLock;
	EState *executorState = NULL;
	CmdType commandType = queryDesc->operation;

	/* ensure that the task is not NULL */
	Assert(task != NULL);

	/* disallow triggers during distributed modify commands */
	if (commandType != CMD_SELECT)
	{
		eflags |= EXEC_FLAG_SKIP_TRIGGERS;

		if (IsTransactionBlock())
		{
			if (shardForTransaction == INVALID_SHARD_ID)
			{
				MemoryContext oldContext = NULL;
				ListCell *placementCell = NULL;

				oldContext = MemoryContextSwitchTo(TopTransactionContext);

				foreach(placementCell, task->taskPlacementList)
				{
					ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
					PGconn *connection = GetOrEstablishConnection(placement->nodeName,
																  placement->nodePort);
					TxnParticipant *txnParticipant = NULL;
					txnParticipant = (TxnParticipant *) palloc(sizeof(TxnParticipant));

					CopyShardPlacement(placement, (ShardPlacement *) txnParticipant);
					txnParticipant->connection = connection;

					if (txnParticipant->connection != NULL)
					{
						participantList = lappend(participantList, txnParticipant);
					}
					else
					{
						failedParticipantList = lappend(participantList, txnParticipant);
					}
				}

				MemoryContextSwitchTo(oldContext);

				shardForTransaction = task->anchorShardId;

				if (!callbackInstalled)
				{
					RegisterXactCallback(RouterTransactionCallback, NULL);
					RegisterSubXactCallback(RouterSubtransactionCallback, NULL);
					callbackInstalled = true;
				}
			}
			else if (shardForTransaction != task->anchorShardId)
			{
				ereport(ERROR, (errmsg("CAN'T CHANGE SHARDS	")));
			}
		}
	}

	/* signal that it is a router execution */
	eflags |= EXEC_FLAG_CITUS_ROUTER_EXECUTOR;

	/* build empty executor state to obtain per-query memory context */
	executorState = CreateExecutorState();
	executorState->es_top_eflags = eflags;
	executorState->es_instrument = queryDesc->instrument_options;

	queryDesc->estate = executorState;

	/*
	 * As it's similar to what we're doing, use a MaterialState node to store
	 * our state. This is used to store our tuplestore, so cursors etc. can
	 * work.
	 */
	queryDesc->planstate = (PlanState *) makeNode(MaterialState);

#if (PG_VERSION_NUM < 90500)

	/* make sure that upsertQuery is false for versions that UPSERT is not available */
	Assert(task->upsertQuery == false);
#endif

	lockMode = CommutativityRuleToLockMode(commandType, task->upsertQuery);

	if (lockMode != NoLock)
	{
		AcquireExecutorShardLock(task, lockMode);
	}
}


/*
 * CommutativityRuleToLockMode determines the commutativity rule for the given
 * command and returns the appropriate lock mode to enforce that rule. The
 * function assumes a SELECT doesn't modify state and therefore is commutative
 * with all other commands. The function also assumes that an INSERT commutes
 * with another INSERT, but not with an UPDATE/DELETE/UPSERT; and an
 * UPDATE/DELETE/UPSERT doesn't commute with an INSERT, UPDATE, DELETE or UPSERT.
 *
 * Note that the above comment defines INSERT INTO ... ON CONFLICT type of queries
 * as an UPSERT. Since UPSERT is not defined as a separate command type in postgres,
 * we have to pass it as a second parameter to the function.
 *
 * The above mapping is overridden entirely when all_modifications_commutative
 * is set to true. In that case, all commands just claim a shared lock. This
 * allows the shard repair logic to lock out modifications while permitting all
 * commands to otherwise commute.
 */
static LOCKMODE
CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery)
{
	LOCKMODE lockMode = NoLock;

	/* bypass commutativity checks when flag enabled */
	if (AllModificationsCommutative)
	{
		return ShareLock;
	}

	if (commandType == CMD_SELECT)
	{
		lockMode = NoLock;
	}
	else if (upsertQuery)
	{
		lockMode = ExclusiveLock;
	}
	else if (commandType == CMD_INSERT)
	{
		lockMode = ShareLock;
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		lockMode = ExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	return lockMode;
}


/*
 * AcquireExecutorShardLock: acquire shard lock needed for execution of
 * a single task within a distributed plan.
 */
static void
AcquireExecutorShardLock(Task *task, LOCKMODE lockMode)
{
	int64 shardId = task->anchorShardId;

	LockShardResource(shardId, lockMode);
}


/*
 * RouterExecutorRun actually executes a single task on a worker.
 */
void
RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count, Task *task)
{
	EState *estate = queryDesc->estate;
	CmdType operation = queryDesc->operation;
	MemoryContext oldcontext = NULL;

	Assert(estate != NULL);
	Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));
	Assert(task != NULL);

	/* we only support default scan direction and row fetch count */
	if (!ScanDirectionIsForward(direction))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("scan directions other than forward scans "
							   "are unsupported")));
	}

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	if (queryDesc->totaltime != NULL)
	{
		InstrStartNode(queryDesc->totaltime);
	}

	if (operation == CMD_INSERT || operation == CMD_UPDATE ||
		operation == CMD_DELETE)
	{
		uint64 affectedRowCount = ExecuteDistributedModify(task);
		estate->es_processed = affectedRowCount;
	}
	else if (operation == CMD_SELECT)
	{
		DestReceiver *destination = queryDesc->dest;
		TupleDesc resultTupleDescriptor = queryDesc->tupDesc;

		ExecuteSingleShardSelect(queryDesc, count, task, estate,
								 resultTupleDescriptor, destination);
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d",
							   (int) operation)));
	}

	if (queryDesc->totaltime != NULL)
	{
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
	}

	MemoryContextSwitchTo(oldcontext);
}


/*
 * ExecuteDistributedModify is the main entry point for modifying distributed
 * tables. A distributed modification is successful if any placement of the
 * distributed table is successful. ExecuteDistributedModify returns the number
 * of modified rows in that case and errors in all others. This function will
 * also generate warnings for individual placement failures.
 */
static uint64
ExecuteDistributedModify(Task *task)
{
	int64 affectedTupleCount = -1;
	List *placementList = task->taskPlacementList;
	int totalPlacements = list_length(placementList);
	int totalFailures = list_length(failedParticipantList);
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	ListCell *failedPlacementCell = NULL;

	/* if taking part in a transaction, use static transaction list */
	if (shardForTransaction != INVALID_SHARD_ID)
	{
		placementList = participantList;

		/* and add a BEGIN statement to the first command */
		if (!txnStarted)
		{
			StringInfo modifiedQueryString = makeStringInfo();
			appendStringInfoString(modifiedQueryString, "BEGIN; ");
			appendStringInfoString(modifiedQueryString, task->queryString);

			task->queryString = modifiedQueryString->data;
			txnStarted = true;
		}
	}

	foreach(taskPlacementCell, placementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;

		PGconn *connection = NULL;
		PGresult *result = NULL;
		char *currentAffectedTupleString = NULL;
		int64 currentAffectedTupleCount = -1;

		Assert(taskPlacement->shardState == FILE_FINALIZED);

		connection = GetOrEstablishConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		result = PQexec(connection, task->queryString);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool raiseError = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			raiseError = SqlStateMatchesCategory(sqlStateString, category);

			if (raiseError)
			{
				/* this placement is already aborted; remove it from participant list */
				participantList = list_delete_ptr(participantList, taskPlacement);

				ReraiseRemoteError(connection, result);
			}
			else
			{
				WarnRemoteError(connection, result);
			}
			PQclear(result);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		currentAffectedTupleString = PQcmdTuples(result);

		/* could throw error if input > MAX_INT64 */
		scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
		Assert(currentAffectedTupleCount >= 0);

#if (PG_VERSION_NUM < 90600)

		/* before 9.6, PostgreSQL used a uint32 for this field, so check */
		Assert(currentAffectedTupleCount <= 0xFFFFFFFF);
#endif

		if ((affectedTupleCount == -1) ||
			(affectedTupleCount == currentAffectedTupleCount))
		{
			affectedTupleCount = currentAffectedTupleCount;
		}
		else
		{
			ereport(WARNING,
					(errmsg("modified " INT64_FORMAT " tuples, but expected to modify "
							INT64_FORMAT, currentAffectedTupleCount, affectedTupleCount),
					 errdetail("modified placement on %s:%d", nodeName, nodePort)));
		}

		PQclear(result);
	}

	/* total failures should count previously failed placements */
	totalFailures += list_length(failedPlacementList);

	if (shardForTransaction != INVALID_SHARD_ID)
	{
		ListCell *placementCell;
		MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

		/*
		 * Placements themselves are already in TopTransactionContext, so they
		 * will live on to the next command; just need to ensure the txn's list
		 * of placements is built in the same context.
		 */
		foreach(placementCell, failedPlacementList)
		{
			ShardPlacement *failedPlacement = lfirst(placementCell);
			failedParticipantList = lappend(failedParticipantList, failedPlacement);

			/* additionally, remove newly failed placements from txn's list */
			participantList = list_delete_ptr(participantList, failedPlacement);
		}

		MemoryContextSwitchTo(oldContext);

		/* invalidation occurs later for txns: clear local failure list */
		failedPlacementList = NIL;
	}

	/* if all placements have failed, error out */
	if (totalFailures == totalPlacements)
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
		uint64 shardLength = 0;

		DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
								failedPlacement->nodePort);
		InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
								failedPlacement->nodeName, failedPlacement->nodePort);
	}

	return (uint64) affectedTupleCount;
}


/*
 * ExecuteTransactionEnd ends any remote transactions still taking place on
 * remote nodes. It uses txnPlacementList to know which placements still need
 * final COMMIT or ABORT commands. Any failures are added to the other list,
 * failedTxnPlacementList, to eventually be marked as failed.
 */
static void
ExecuteTransactionEnd(char *sqlCommand)
{
	ListCell *participantCell = NULL;

	foreach(participantCell, participantList)
	{
		TxnParticipant *participant = (TxnParticipant *) lfirst(participantCell);
		PGconn *connection = participant->connection;
		PGresult *result = NULL;

		result = PQexec(connection, sqlCommand);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			WarnRemoteError(connection, result);

			failedParticipantList = lappend(failedParticipantList, participant);
		}

		PQclear(result);
	}
}


/*
 * ExecuteSingleShardSelect executes, if not done already, the remote select query and
 * sends the resulting tuples to the given destination receiver. If the query fails on a
 * given placement, the function attempts it on its replica.
 */
static void
ExecuteSingleShardSelect(QueryDesc *queryDesc, uint64 tupleCount, Task *task,
						 EState *executorState, TupleDesc tupleDescriptor,
						 DestReceiver *destination)
{
	bool resultsOK = false;
	TupleTableSlot *tupleTableSlot = NULL;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	Tuplestorestate *tupleStore = routerState->tuplestorestate;
	uint64 currentTupleCount = 0;

	/* initialize tuplestore for the first call */
	if (routerState->tuplestorestate == NULL)
	{
		routerState->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
		tupleStore = routerState->tuplestorestate;

		resultsOK = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
		if (!resultsOK)
		{
			ereport(ERROR, (errmsg("could not receive query results")));
		}
	}

	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	/* startup the tuple receiver */
	(*destination->rStartup)(destination, CMD_SELECT, tupleDescriptor);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		(*destination->receiveSlot)(tupleTableSlot, destination);
		executorState->es_processed++;

		ExecClearTuple(tupleTableSlot);

		currentTupleCount++;

		/*
		 * If numberTuples is zero fetch all tuples, otherwise stop after
		 * count tuples.
		 */
		if (tupleCount > 0 && tupleCount == currentTupleCount)
		{
			break;
		}
	}

	/* shutdown the tuple receiver */
	(*destination->rShutdown)(destination);

	ExecDropSingleTupleTableSlot(tupleTableSlot);
}


/*
 * ExecuteTaskAndStoreResults executes the task on the remote node, retrieves
 * the results and stores them in the given tuple store. If the task fails on
 * one of the placements, the function retries it on other placements.
 */
bool
ExecuteTaskAndStoreResults(Task *task, TupleDesc tupleDescriptor,
						   Tuplestorestate *tupleStore)
{
	bool resultsOK = false;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;
		bool queryOK = false;
		bool storedOK = false;

		PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, task->queryString);
		if (!queryOK)
		{
			PurgeConnection(connection);
			continue;
		}

		storedOK = StoreQueryResult(connection, tupleDescriptor, tupleStore);
		if (storedOK)
		{
			resultsOK = true;
			break;
		}
		else
		{
			tuplestore_clear(tupleStore);
			PurgeConnection(connection);
		}
	}

	return resultsOK;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(PGconn *connection, char *query)
{
	int querySent = 0;
	int singleRowMode = 0;

	querySent = PQsendQuery(connection, query);
	if (querySent == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection);
	if (singleRowMode == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	return true;
}


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results and stores them in the given tuple-store. If the
 * function can't receive query results, it returns false. Note that this
 * function assumes the query has already been sent on the connection and the
 * tuplestore has earlier been initialized.
 */
static bool
StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
				 Tuplestorestate *tupleStore)
{
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"StoreQueryResult",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	Assert(tupleStore != NULL);

	for (;;)
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection);
		if (result == NULL)
		{
			break;
		}

		resultStatus = PQresultStatus(result);
		if ((resultStatus != PGRES_SINGLE_TUPLE) && (resultStatus != PGRES_TUPLES_OK))
		{
			WarnRemoteError(connection, result);
			PQclear(result);

			return false;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);
		Assert(columnCount == expectedColumnCount);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);
		}

		PQclear(result);
	}

	pfree(columnArray);

	return true;
}


/*
 * RouterExecutorFinish cleans up after a distributed execution.
 */
void
RouterExecutorFinish(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	Assert(estate != NULL);

	estate->es_finished = true;
}


/*
 * RouterExecutorEnd cleans up the executor state after a distributed
 * execution.
 */
void
RouterExecutorEnd(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;

	if (routerState->tuplestorestate)
	{
		tuplestore_end(routerState->tuplestorestate);
	}

	Assert(estate != NULL);

	FreeExecutorState(estate);
	queryDesc->estate = NULL;
	queryDesc->totaltime = NULL;
}


/*
 * RouterTransactionCallback handles committing or aborting remote transactions
 * after the local one has committed or aborted. It only sends COMMIT or ABORT
 * commands to still-healthy remotes; the failed ones are marked as inactive if
 * after a successful COMMIT (no need to mark on ABORTs).
 */
static void
RouterTransactionCallback(XactEvent event, void *arg)
{
	if (!txnStarted)
	{
		return;
	}

	switch (event)
	{
		/* after the local transaction commits, commit the remote ones */
		case XACT_EVENT_COMMIT:
#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_COMMIT:
#endif
		{
			ListCell *failedPlacementCell = NULL;

			ExecuteTransactionEnd("COMMIT TRANSACTION");

			/* now mark all failed placements inactive */
			foreach(failedPlacementCell, failedParticipantList)
			{
				ShardPlacement *failedPlacement = NULL;
				failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
				uint64 shardLength = 0;

				DeleteShardPlacementRow(failedPlacement->shardId,
										failedPlacement->nodeName,
										failedPlacement->nodePort);
				InsertShardPlacementRow(failedPlacement->shardId,
										FILE_INACTIVE, shardLength,
										failedPlacement->nodeName,
										failedPlacement->nodePort);
			}

			break;
		}

		/* if the local transaction aborts, send ABORT to healthy remotes */
		case XACT_EVENT_ABORT:
#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_ABORT:
#endif
		{
			ExecuteTransactionEnd("ABORT TRANSACTION");

			break;
		}

		/* no support for prepare with long-running transactions */
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_PREPARE:
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot prepare a transaction that modified "
								   "distributed tables")));

			break;
		}

		/* we don't have any work to do pre-commit */
		case XACT_EVENT_PRE_COMMIT:
#if (PG_VERSION_NUM >= 90500)
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif
		{
			/* leave early to avoid resetting transaction state */
			return;
		}
	}

	/* reset transaction state */
	shardForTransaction = INVALID_SHARD_ID;
	participantList = NIL;
	failedParticipantList = NIL;
	txnStarted = false;
}


/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
							 SubTransactionId parentSubid, void *arg)
{
	if (txnStarted && event == SUBXACT_EVENT_ABORT_SUB)
	{
		UnregisterSubXactCallback(RouterSubtransactionCallback, NULL);

		AbortOutOfAnyTransaction();

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot ROLLBACK in transactions which modify "
							   "distributed tables")));
	}
}
