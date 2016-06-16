/*-------------------------------------------------------------------------
 *
 * shard_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SHARD_TRANSACTION_H
#define SHARD_TRANSACTION_H


#include "access/xact.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"


/* ShardConnections represents a set of connections for each placement of a shard */
typedef struct ShardConnections
{
	int64 shardId;
	List *connectionList;
} ShardConnections;


extern List *shardPlacementConnectionList;


extern HTAB * CreateShardConnectionHash(void);
extern ShardConnections * GetShardConnections(HTAB *shardConnectionHash,
											  int64 shardId,
											  bool *shardConnectionsFound);
extern void OpenConnectionsToShardPlacements(uint64 shardId, HTAB *shardConnectionHash,
											 char *nodeUser);
extern List * ConnectionList(HTAB *connectionHash);
extern void CloseConnections(List *connectionList);
extern HTAB * OpenTransactionsToAllShardPlacements(List *shardIdList,
												   char *relationOwner);
extern void CompleteShardPlacementTransactions(XactEvent event, void *arg);

#endif /* SHARD_TRANSACTION_H */
