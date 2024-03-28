#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
	RM_TableData *rel;
	void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData); //it is made
extern RC shutdownRecordManager (); //it is made 
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel); //it is made

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record); //it is made

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond); //It is made 
extern RC next (RM_ScanHandle *scan, Record *record); //It is made 
extern RC closeScan (RM_ScanHandle *scan); //it is made

// dealing with schemas
extern int getRecordSize (Schema *schema); //it is made 
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys); //it is made 
extern RC freeSchema (Schema *schema); //it is made

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema); //it is made
extern RC freeRecord (Record *record);// it is made 
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value); //it is made

#endif // RECORD_MGR_H
