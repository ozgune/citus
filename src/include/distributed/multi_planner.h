/*-------------------------------------------------------------------------
 *
 * multi_planner.h
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PLANNER_H
#define MULTI_PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"


typedef struct RelationRestrictionContext
{
	bool hasDistributedRelation;
	bool hasLocalRelation;
	List *relationRestrictionList;
} RelationRestrictionContext;

typedef struct RelationRestriction
{
	Index index;
	Oid relationId;
	bool distributedRelation;
	RangeTblEntry *rte;
	RelOptInfo *relOptInfo;
	PlannerInfo *plannerInfo;
} RelationRestriction;


extern PlannedStmt * multi_planner(Query *parse, int cursorOptions,
								   ParamListInfo boundParams);

extern bool HasCitusToplevelNode(PlannedStmt *planStatement);
struct MultiPlan;
extern struct MultiPlan * CreatePhysicalPlan(Query *originalQuery, Query *query, ParamListInfo boundParams, RelationRestrictionContext *restrictionContext);
extern struct MultiPlan * GetMultiPlan(PlannedStmt *planStatement);
extern PlannedStmt * MultiQueryContainerNode(PlannedStmt *result,
											 struct MultiPlan *multiPlan);
extern void multi_set_rel_pathlist(PlannerInfo *root, RelOptInfo *relOptInfo,
								   Index index, RangeTblEntry *rte);

#endif /* MULTI_PLANNER_H */
