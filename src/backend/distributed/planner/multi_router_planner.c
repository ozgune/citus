/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan single shard queries
 * including distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600)
#include "access/stratnum.h"
#else
#include "access/skey.h"
#endif
#include "access/xact.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#if (PG_VERSION_NUM >= 90500)
#include "nodes/makefuncs.h"
#endif
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/typcache.h"


/* planner functions forward declarations */
static Task * RouterModifyTask(Query *query);
#if (PG_VERSION_NUM >= 90500)
static OnConflictExpr * RebuildOnConflict(Oid relationId,
										  OnConflictExpr *originalOnConflict);
#endif
static ShardInterval * TargetShardInterval(Query *query);
static List * NewTargetShardInterval(Query *query,
											  RelationRestrictionContext *restrictionContext);
static List * QueryRestrictList(Query *query);
static bool FastShardPruningPossible(CmdType commandType, char partitionMethod);
static ShardInterval * FastShardPruning(Oid distributedTableId,
										Const *partionColumnValue);
static Oid ExtractFirstDistributedTableId(Query *query);
static Const * ExtractInsertPartitionValue(Query *query, Var *partitionColumn);
static Task * RouterSelectTask(Query *originalQuery, Query *query,
							   RelationRestrictionContext *restrictionContext);
static void UpdateJoinTreeAndRelationNames(Query *query,
										   RelationRestrictionContext *restrictionContext,
										   List *prunedRelationShardList);
static Job * RouterQueryJob(Query *query, Task *task);
static bool MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType);
static void SetRangeTablesInherited(Query *query);


/*
 * MultiRouterPlanCreate creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single shard. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then the function
 * returns NULL.
 */
MultiPlan *
MultiRouterPlanCreate(Query *originalQuery, Query *query,
					  MultiExecutorType taskExecutorType,
					  RelationRestrictionContext *restrictionContext)
{
	Task *task = NULL;
	Job *job = NULL;
	MultiPlan *multiPlan = NULL;
	CmdType commandType = query->commandType;
	bool modifyTask = false;
	Query *jobQuery = NULL;

	bool routerPlannable = MultiRouterPlannableQuery(query, taskExecutorType);
	if (!routerPlannable)
	{
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		modifyTask = true;
	}

	if (modifyTask)
	{
		ErrorIfModifyQueryNotSupported(query);
		task = RouterModifyTask(query);
		jobQuery = query;
	}
	else
	{
		Assert(commandType == CMD_SELECT);

		task = RouterSelectTask(originalQuery, query, restrictionContext);
		jobQuery = originalQuery;
	}

	if (task == NULL)
	{
		return NULL;
	}

	job = RouterQueryJob(jobQuery, task);

	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->masterTableName = NULL;

	return multiPlan;
}


/*
 * ErrorIfModifyQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
void
ErrorIfModifyQueryNotSupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool hasNonConstTargetEntryExprs = false;
	bool hasNonConstQualExprs = false;
	bool specifiesPartitionValue = false;
#if (PG_VERSION_NUM >= 90500)
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;
#endif

	CmdType commandType = queryTree->commandType;
	Assert(commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		   commandType == CMD_DELETE);

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Subqueries are not supported in distributed"
								  " modifications.")));
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Common table expressions are not supported in"
								  " distributed modifications.")));
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else
		{
			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			char *rangeTableEntryErrorDetail = NULL;
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				rangeTableEntryErrorDetail = "Subqueries are not supported in"
											 " distributed modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_JOIN)
			{
				rangeTableEntryErrorDetail = "Joins are not supported in distributed"
											 " modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_FUNCTION)
			{
				rangeTableEntryErrorDetail = "Functions must not appear in the FROM"
											 " clause of a distributed modifications.";
			}
			else
			{
				rangeTableEntryErrorDetail = "Unrecognized range table entry.";
			}

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given"
								   " modifications"),
							errdetail("%s", rangeTableEntryErrorDetail)));
		}
	}

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Joins are not supported in distributed "
								  "modifications.")));
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Multi-row INSERTs to distributed tables are not "
								  "supported.")));
	}

	/* reject queries with a returning list */
	if (list_length(queryTree->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("RETURNING clauses are not supported in distributed "
								  "modifications.")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		FromExpr *joinTree = NULL;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (contain_mutable_functions((Node *) targetEntry->expr))
			{
				hasNonConstTargetEntryExprs = true;
			}

			if (commandType == CMD_UPDATE &&
				targetEntry->resno == partitionColumn->varattno)
			{
				specifiesPartitionValue = true;
			}
		}

		joinTree = queryTree->jointree;
		if (joinTree != NULL && contain_mutable_functions(joinTree->quals))
		{
			hasNonConstQualExprs = true;
		}
	}

#if (PG_VERSION_NUM >= 90500)
	if (commandType == CMD_INSERT && queryTree->onConflict != NULL)
	{
		onConflictSet = queryTree->onConflict->onConflictSet;
		arbiterWhere = queryTree->onConflict->arbiterWhere;
		onConflictWhere = queryTree->onConflict->onConflictWhere;
	}

	/*
	 * onConflictSet is expanded via expand_targetlist() on the standard planner.
	 * This ends up adding all the columns to the onConflictSet even if the user
	 * does not explicitly state the columns in the query.
	 *
	 * The following loop simply allows "DO UPDATE SET part_col = table.part_col"
	 * types of elements in the target list, which are added by expand_targetlist().
	 * Any other attempt to update partition column value is forbidden.
	 */
	foreach(setTargetCell, onConflictSet)
	{
		TargetEntry *setTargetEntry = (TargetEntry *) lfirst(setTargetCell);

		if (setTargetEntry->resno == partitionColumn->varattno)
		{
			Expr *setExpr = setTargetEntry->expr;
			if (IsA(setExpr, Var) &&
				((Var *) setExpr)->varattno == partitionColumn->varattno)
			{
				specifiesPartitionValue = false;
			}
			else
			{
				specifiesPartitionValue = true;
			}
		}
		else
		{
			/*
			 * Similarly, allow  "DO UPDATE SET col_1 = table.col_1" types of
			 * target list elements. Note that, the following check allows
			 * "DO UPDATE SET col_1 = table.col_2", which is not harmful.
			 */
			if (IsA(setTargetEntry->expr, Var))
			{
				continue;
			}
			else if (contain_mutable_functions((Node *) setTargetEntry->expr))
			{
				hasNonConstTargetEntryExprs = true;
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		hasNonConstQualExprs = true;
	}
#endif

	if (hasNonConstTargetEntryExprs || hasNonConstQualExprs)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("functions used in modification queries on distributed "
							   "tables must be marked IMMUTABLE")));
	}

	if (specifiesPartitionValue)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
}


/*
 * RouterModifyTask builds a Task to represent a modification performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Task *
RouterModifyTask(Query *query)
{
	ShardInterval *shardInterval = TargetShardInterval(query);
	uint64 shardId = shardInterval->shardId;
	FromExpr *joinTree = NULL;
	StringInfo queryString = makeStringInfo();
	Task *modifyTask = NULL;
	bool upsertQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query. This applies to SELECT, UPDATE and
	 * DELETE statements.
	 */
	joinTree = query->jointree;
	if ((joinTree != NULL) && (joinTree->quals != NULL))
	{
		Node *whereClause = joinTree->quals;
		if (IsA(whereClause, List))
		{
			joinTree->quals = (Node *) make_ands_explicit((List *) whereClause);
		}
	}

#if (PG_VERSION_NUM >= 90500)
	if (query->onConflict != NULL)
	{
		RangeTblEntry *rangeTableEntry = NULL;
		Oid relationId = shardInterval->relationId;

		/* set the flag */
		upsertQuery = true;

		/* setting an alias simplifies deparsing of UPSERTs */
		rangeTableEntry = linitial(query->rtable);
		if (rangeTableEntry->alias == NULL)
		{
			Alias *alias = makeAlias(UPSERT_ALIAS, NIL);
			rangeTableEntry->alias = alias;
		}

		/* some fields in onConflict expression needs to be updated for deparsing */
		query->onConflict = RebuildOnConflict(relationId, query->onConflict);
	}
#else

	/* always set to false for PG_VERSION_NUM < 90500 */
	upsertQuery = false;
#endif

	/*
	 * We set inh flag of all range tables entries to true so that deparser will not
	 * add ONLY keyword to resulting query string.
	 */
	SetRangeTablesInherited(query);

	deparse_shard_query(query, shardInterval->relationId, shardId, queryString);
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	modifyTask = CitusMakeNode(Task);
	modifyTask->jobId = INVALID_JOB_ID;
	modifyTask->taskId = INVALID_TASK_ID;
	modifyTask->taskType = MODIFY_TASK;
	modifyTask->queryString = queryString->data;
	modifyTask->anchorShardId = shardId;
	modifyTask->dependedTaskList = NIL;
	modifyTask->upsertQuery = upsertQuery;

	return modifyTask;
}


#if (PG_VERSION_NUM >= 90500)

/*
 * RebuildOnConflict rebuilds OnConflictExpr for correct deparsing. The function
 * makes WHERE clause elements explicit and filters dropped columns
 * from the target list.
 */
static OnConflictExpr *
RebuildOnConflict(Oid relationId, OnConflictExpr *originalOnConflict)
{
	OnConflictExpr *updatedOnConflict = copyObject(originalOnConflict);
	Node *onConflictWhere = updatedOnConflict->onConflictWhere;
	List *onConflictSet = updatedOnConflict->onConflictSet;
	TupleDesc distributedRelationDesc = NULL;
	ListCell *targetEntryCell = NULL;
	List *filteredOnConflictSet = NIL;
	Form_pg_attribute *tableAttributes = NULL;
	Relation distributedRelation = RelationIdGetRelation(relationId);

	/* Convert onConflictWhere qualifiers to an explicitly and'd clause */
	updatedOnConflict->onConflictWhere =
		(Node *) make_ands_explicit((List *) onConflictWhere);

	/*
	 * Here we handle dropped columns on the distributed table. onConflictSet
	 * includes the table attributes even if they are dropped,
	 * since the it is expanded via expand_targetlist() on standard planner.
	 */

	/* get the relation tuple descriptor and table attributes */
	distributedRelationDesc = RelationGetDescr(distributedRelation);
	tableAttributes = distributedRelationDesc->attrs;

	foreach(targetEntryCell, onConflictSet)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		FormData_pg_attribute *tableAttribute = tableAttributes[targetEntry->resno - 1];

		/* skip dropped columns */
		if (tableAttribute->attisdropped)
		{
			continue;
		}

		/* we only want to deparse non-dropped columns */
		filteredOnConflictSet = lappend(filteredOnConflictSet, targetEntry);
	}

	/* close distributedRelation to prevent leaks */
	RelationClose(distributedRelation);

	/* set onConflictSet again with the filtered list */
	updatedOnConflict->onConflictSet = filteredOnConflictSet;

	return updatedOnConflict;
}


#endif


/*
 * TargetShardInterval determines the single shard targeted by a provided command.
 * If no matching shards exist, or if the modification targets more than one one
 * shard, this function raises an error depending on the command type.
 */
static ShardInterval *
TargetShardInterval(Query *query)
{
	CmdType commandType = query->commandType;
	bool selectTask = (commandType == CMD_SELECT);
	List *prunedShardList = NIL;
	int prunedShardCount = 0;


	int shardCount = 0;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	bool fastShardPruningPossible = false;

	/* error out if no shards exist for the table */
	shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	fastShardPruningPossible = FastShardPruningPossible(query->commandType,
														partitionMethod);
	if (fastShardPruningPossible)
	{
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);
		ShardInterval *shardInterval = FastShardPruning(distributedTableId,
														partitionValue);

		if (shardInterval != NULL)
		{
			prunedShardList = lappend(prunedShardList, shardInterval);
		}
	}
	else
	{
		List *restrictClauseList = QueryRestrictList(query);
		Index tableId = 1;
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);

		prunedShardList = PruneShardList(distributedTableId, tableId, restrictClauseList,
										 shardIntervalList);
	}

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		if (selectTask)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("router executor queries must target exactly one "
								   "shard")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributed modifications must target exactly one "
								   "shard")));
		}
	}

	return (ShardInterval *) linitial(prunedShardList);
}

static List *
NewTargetShardInterval(Query *query, RelationRestrictionContext *restrictionContext)
{
	List *prunedRelationShardList = NIL;
	int shardCount = 0;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	bool fastShardPruningPossible = false;

	/* error out if no shards exist for the table */
	shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		return NULL;
	}

	fastShardPruningPossible = FastShardPruningPossible(query->commandType,
														partitionMethod);
	if (fastShardPruningPossible)
	{
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);
		ShardInterval *shardInterval = FastShardPruning(distributedTableId,
														partitionValue);
		List *prunedShardList = NIL;

		if (shardInterval != NULL)
		{
			prunedShardList = lappend(prunedShardList, shardInterval);
		}

		prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
	}
	else if (restrictionContext != NULL)
	{
		ListCell *restrictionCell = NULL;
		foreach(restrictionCell, restrictionContext->relationRestrictionList)
		{
			RelationRestriction *relationResriction = (RelationRestriction *) lfirst(restrictionCell);
			Index tableId = relationResriction->index;
			List *baseRestrictionList = relationResriction->relOptInfo->baserestrictinfo;
			List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
			Oid relationId = relationResriction->relationId;
			List *shardIntervalList = LoadShardIntervalList(relationId);
			List *prunedShardList = PruneShardList(distributedTableId, tableId,
												   restrictClauseList,
												   shardIntervalList);

			prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
		}
	}
	else
	{
		List *restrictClauseList = QueryRestrictList(query);
		Index tableId = 1;
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);

		List *prunedShardList = PruneShardList(distributedTableId, tableId,
											   restrictClauseList, shardIntervalList);

		prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
	}

	return prunedRelationShardList;
}

/*
 * UseFastShardPruning returns true if the commandType is INSERT and partition method
 * is hash or range.
 */
static bool
FastShardPruningPossible(CmdType commandType, char partitionMethod)
{
	/* we currently only support INSERTs */
	if (commandType != CMD_INSERT)
	{
		return false;
	}

	/* fast shard pruning is only supported for hash and range partitioned tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		return true;
	}

	return false;
}


/*
 * FastShardPruning is a higher level API for FindShardInterval function. Given the relationId
 * of the distributed table and partitionValue, FastShardPruning function finds the corresponding
 * shard interval that the partitionValue should be in. FastShardPruning returns NULL if no
 * ShardIntervals exist for the given partitionValue.
 */
static ShardInterval *
FastShardPruning(Oid distributedTableId, Const *partitionValue)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	int shardCount = cacheEntry->shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;
	bool useBinarySearch = false;
	char partitionMethod = cacheEntry->partitionMethod;
	FmgrInfo *shardIntervalCompareFunction = cacheEntry->shardIntervalCompareFunction;
	bool hasUniformHashDistribution = cacheEntry->hasUniformHashDistribution;
	FmgrInfo *hashFunction = NULL;
	ShardInterval *shardInterval = NULL;

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH || !hasUniformHashDistribution)
	{
		useBinarySearch = true;
	}

	/* we only need hash functions for hash distributed tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		hashFunction = cacheEntry->hashFunction;
	}

	/*
	 * Call FindShardInterval to find the corresponding shard interval for the
	 * given partition value.
	 */
	shardInterval = FindShardInterval(partitionValue->constvalue,
									  sortedShardIntervalArray, shardCount,
									  partitionMethod,
									  shardIntervalCompareFunction, hashFunction,
									  useBinarySearch);

	return shardInterval;
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 */
static List *
QueryRestrictList(Query *query)
{
	List *queryRestrictList = NIL;
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT)
	{
		/* build equality expression based on partition column value for row */
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE ||
			 commandType == CMD_SELECT)
	{
		queryRestrictList = WhereClauseList(query->jointree);
	}

	return queryRestrictList;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
static Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of an INSERT command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Const *
ExtractInsertPartitionValue(Query *query, Var *partitionColumn)
{
	Const *partitionValue = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		Assert(IsA(targetEntry->expr, Const));

		partitionValue = (Const *) targetEntry->expr;
	}

	if (partitionValue == NULL || partitionValue->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return partitionValue;
}


/* RouterSelectTask builds a Task to represent a single shard select query */
static Task *
RouterSelectTask(Query *originalQuery, Query *query,
				 RelationRestrictionContext *restrictionContext)
{
	Task *task = NULL;
	List * prunedRelationShardList = NewTargetShardInterval(query, restrictionContext);
	StringInfo queryString = makeStringInfo();
	uint64 shardId = INVALID_SHARD_ID;
	bool upsertQuery = false;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = query->commandType;
	ListCell *prunedRelationShardListCell = NULL;

	if (prunedRelationShardList == NULL)
	{
		return NULL;
	}

	Assert(commandType == CMD_SELECT);

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);
		ListCell *prunedShardCell = NULL;

		foreach(prunedShardCell, prunedShardList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(prunedShardCell);
			ereport(WARNING, (errmsg("Shard Id %d of %d", (int) shardInterval->shardId,
									list_length(prunedShardList))));

			if (shardId == INVALID_SHARD_ID)
			{
				shardId = shardInterval->shardId;
			}
		}
	}

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);

		if (list_length(prunedShardList) != 1)
		{
			return NULL;
		}
	}


	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query.
	 */

	/* unwrap extra fromexpr introduced by standard planner */

	UpdateJoinTreeAndRelationNames(query, restrictionContext, prunedRelationShardList);

	pg_get_query_def(query, queryString);
	ereport(WARNING, (errmsg("distributed statement: %s", queryString->data)));

	task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->taskType = ROUTER_TASK;
	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->dependedTaskList = NIL;
	task->upsertQuery = upsertQuery;

	return task;
}


static void
UpdateJoinTreeAndRelationNames(Query* query,
							   RelationRestrictionContext *restrictionContext,
							   List* prunedRelationShardList)
{
	bool doneUnwrapping = false;
	uint64 shardId = INVALID_SHARD_ID;
	Oid relationId = InvalidOid;
	Oid schemaId = InvalidOid;
	FromExpr *joinTree = NULL;
	List *rangeTableIndexList = NIL;
	ListCell *rangeTableIndexCell = NULL;
	char *relationName = NULL;
	char *schemaName = NULL;
	int relationCount = list_length(restrictionContext->relationRestrictionList);

	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query.
	 */
	/* unwrap extra fromexpr introduced by standard planner */
	joinTree = query->jointree;
	doneUnwrapping = false;
	while (joinTree != NULL && !doneUnwrapping)
	{
		doneUnwrapping = true;
		if (list_length(joinTree->fromlist) == 1)
		{
			Node *innerExpr = (Node *) linitial(joinTree->fromlist);
			if (IsA(innerExpr, FromExpr))
			{
				joinTree = (FromExpr *) innerExpr;
				doneUnwrapping = false;
			}
		}
	}
	query->jointree = joinTree;

	if ((joinTree != NULL) && (joinTree->quals != NULL) && IsA(joinTree->quals, List))
	{
		Node *whereClause = (Node *) make_ands_explicit(
				(List *) joinTree->quals);
		joinTree->quals = whereClause;
	}
	ExtractRangeTableIndexWalker((Node*) query->jointree, &rangeTableIndexList);
	foreach(rangeTableIndexCell, rangeTableIndexList)
	{
		Index index = lfirst_int(rangeTableIndexCell);
		RangeTblEntry *rte = rt_fetch(index, query->rtable);
		RTEKind rteKind = rte->rtekind;
		ListCell *relationRestrictionCell = NULL;
		RelationRestriction *relationRestriction = NULL;
		int restrictionOffset = 0;
		List *shardIntervalList = NIL;
		ShardInterval *shardInterval = NULL;


		if (rteKind == RTE_SUBQUERY)
		{
			UpdateJoinTreeAndRelationNames(rte->subquery, restrictionContext, prunedRelationShardList);
		}
		else if (rteKind == RTE_RELATION)
		{

			foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
			{
				relationRestriction = (RelationRestriction *) lfirst(
						relationRestrictionCell);

				if (relationRestriction->index == index)
				{
					break;
				}
				restrictionOffset++;
				relationRestriction = NULL;
			}

			Assert(relationRestriction != NULL);
			Assert(restrictionOffset >= 0);
			Assert(restrictionOffset < relationCount);

			shardIntervalList = (List *) list_nth(prunedRelationShardList,
					restrictionOffset);

			Assert(list_length(shardIntervalList) == 1);
			shardInterval = (ShardInterval *) linitial(shardIntervalList);

			shardId = shardInterval->shardId;

			relationId = shardInterval->relationId;

			relationName = get_rel_name(relationId);
			AppendShardIdToName(&relationName, shardId);

			schemaId = get_rel_namespace(relationId);
			schemaName = get_namespace_name(schemaId);
			if (strncmp(schemaName, "public", NAMEDATALEN) == 0)
			{
				schemaName = NULL;
			}

			ModifyRangeTblExtraData(rte, CITUS_RTE_SHARD, schemaName,
					relationName,
					NIL);
		}
		else
		{
			ereport(ERROR, (errmsg("Unexpected RTE Kind %d", (int) rteKind)));
		}
	}
}


/*
 * RouterQueryJob creates a Job for the specified query to execute the
 * provided single shard select task.
 */
static Job *
RouterQueryJob(Query *query, Task *task)
{
	Job *job = NULL;
	List *taskList = NIL;
	TaskType taskType = task->taskType;

	/*
	 * We send modify task to the first replica, otherwise we choose the target shard
	 * according to task assignment policy.
	 */
	if (taskType == MODIFY_TASK)
	{
		taskList = FirstReplicaAssignTaskList(list_make1(task));
	}
	else
	{
		taskList = AssignAnchorShardTaskList(list_make1(task));
	}

	job = CitusMakeNode(Job);
	job->dependedJobList = NIL;
	job->jobId = INVALID_JOB_ID;
	job->subqueryPushdown = false;
	job->jobQuery = query;
	job->taskList = taskList;

	return job;
}


/*
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a select query issued on a hash
 * partitioned distributed table, and it has a exact match comparison on the
 * partition column. This feature is enabled if task executor is set to real-time
 */
bool
MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType)
{
	Oid distributedTableId = InvalidOid;
	char partitionMethod = '\0';
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = query->commandType;
	FromExpr *joinTree = query->jointree;
	List *rangeTableIndexList = NIL;
	ListCell *rangeTableIndexCell = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	if (taskExecutorType != MULTI_EXECUTOR_REAL_TIME)
	{
		return false;
	}

	Assert(commandType == CMD_SELECT);

	ereport(WARNING, (errmsg("Join Tree : %s", nodeToString(query->jointree))));

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which are recursive, with CommonTableExpr, with locking (hasForUpdate),
	 * or with window functions are also rejected here.
	 * Queries which have subqueries, or tablesamples in FROM clauses are rejected later
	 * during RangeTblEntry checks.
	 */
	if (query->hasSubLinks == true || query->cteList != NIL || query->hasForUpdate ||
		query->hasRecursive)
	{
		return false;
	}

#if (PG_VERSION_NUM >= 90500)
	if (query->groupingSets)
	{
		return false;
	}
#endif

	/* extract range table entries */
	ExtractRangeTableIndexWalker((Node *) query, &rangeTableIndexList);
	foreach(rangeTableIndexCell, rangeTableIndexList)
	{
		Index index = lfirst_int(rangeTableIndexCell);
		RangeTblEntry *rte = rt_fetch(index, query->rtable);

		if (rte->rtekind == RTE_RELATION)
		{
			/* only hash partitioned tables are supported */
			distributedTableId = rte->relid;
			partitionMethod = PartitionMethod(distributedTableId);

			if (partitionMethod != DISTRIBUTE_BY_HASH)
			{
				return false;
			}
		}
		else
		{
			ereport(WARNING, (errmsg("unsupported rte kind : %d, %s", (int) rte->rtekind,
									nodeToString(rte))));
			return false;
		}

#if (PG_VERSION_NUM >= 90500)
		if (rte->tablesample)
		{
			return false;
		}
#endif

	}

	if (joinTree == NULL)
	{
		return false;
	}

	return true;
}


/*
 * RouterSetRangeTablesInherited sets inh flag of all range table entries to true.
 * We basically iterate over all range table entries and set their inh flag.
 */
static void
SetRangeTablesInherited(Query *query)
{
	List *rangeTableList = query->rtable;
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			rangeTableEntry->inh = true;
		}
	}
}
