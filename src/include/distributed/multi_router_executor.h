/*
 * multi_router_executor.h
 *
 * Function declarations used in executing distributed execution
 * plan.
 *
 */

#ifndef MULTI_ROUTER_EXECUTOR_H_
#define MULTI_ROUTER_EXECUTOR_H_

#include "libpq-fe.h"

#include "distributed/master_metadata_utility.h"
#include "distributed/multi_physical_planner.h"
#include "executor/execdesc.h"

extern bool AllModificationsCommutative;

typedef struct TxnParticipant
{
	ShardPlacement placement;
	PGconn *connection;
} TxnParticipant;


extern void RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task);
extern void RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count,
							  Task *task);
extern bool ExecuteTaskAndStoreResults(Task *task, TupleDesc tupleDescriptor,
									   Tuplestorestate *tupleStore);
extern void RouterExecutorFinish(QueryDesc *queryDesc);
extern void RouterExecutorEnd(QueryDesc *queryDesc);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
