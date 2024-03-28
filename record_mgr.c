#include "record_mgr.h" 
#include <stdlib.h>
#include <string.h>
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// This is custom data structure defined for making the use of Record Manager.
typedef struct RecordManager
{
	// Buffer Manager's PageHandle for using Buffer Manager to access Page files
	BM_PageHandle pageHandle;	// Buffer Manager PageHandle 
	// Buffer Manager's Buffer Pool for using Buffer Manager	
	BM_BufferPool bufferPool;
	// Record ID	
	RID recordID;
	// This variable defines the condition for scanning the records in the table
	Expr *condition;
	// This variable stores the total number of tuples in the table
	int tuplesCount;
	// This variable stores the location of first free page which has empty slots in table
	int freePage;
	// This variable stores the count of the number of records scanned
	int scanCount;
} RecordManager;

const int MAX_NUMBER_OF_PAGES = 100;
const int ATTRIBUTE_SIZE = 15; // Size of the name of the attribute

RecordManager *recordManager;

// ******** CUSTOM FUNCTIONS ******** //

// This function returns a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
	int i, totalSlots = PAGE_SIZE / recordSize; 

	for (i = 0; i < totalSlots; i++)
		if (data[i * recordSize] != '+')
			return i;
	return -1;
}


#pragma region Table and Manager


// Database Management Functions
RC initRecordManager(void *mgmtData) {

    //init the StorageManager
    initStorageManager();

    printf(" The Record Manager is initalizated sucessfully\n");
    
    return RC_OK;
}

// This functions shuts down the Record Manager
extern RC shutdownRecordManager ()
{   
    // Check if the recordManager is not NULL before attempting to free it
    if (recordManager != NULL)
    {
        // Free the allocated memory for recordManager
        free(recordManager);

        // Set the pointer to NULL to avoid dangling pointer issues
        recordManager = NULL;
    }
    return RC_OK;
}

extern RC createTable(char *name, Schema *schema) {
    recordManager = (RecordManager*) malloc(sizeof(RecordManager));

    // Inicializando el buffer pool con la política de reemplazo LFU
    initBufferPool(&recordManager->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

    char data[PAGE_SIZE];
    memset(data, 0, PAGE_SIZE);
    char *pageHandle = data;

    // Establecer el número de tuplas a 0
    *(int*)pageHandle = 0; 
    pageHandle += sizeof(int);

    // Establecer la primera página a 1 (0 es para esquema y metadatos)
    *(int*)pageHandle = 1;
    pageHandle += sizeof(int);

    // Escribir el número de atributos
    *(int*)pageHandle = schema->numAttr;
    pageHandle += sizeof(int);

    // Escribir el tamaño de la clave
    *(int*)pageHandle = schema->keySize;
    pageHandle += sizeof(int);

    int k;
    for(k = 0; k < schema->numAttr; k++) {
        // Escribir el nombre del atributo
        strncpy(pageHandle, schema->attrNames[k], ATTRIBUTE_SIZE);
        pageHandle += ATTRIBUTE_SIZE;

        // Escribir el tipo de dato del atributo
        *(int*)pageHandle = (int)schema->dataTypes[k];
        pageHandle += sizeof(int);

        // Escribir la longitud del tipo de dato
        *(int*)pageHandle = (int) schema->typeLength[k];
        pageHandle += sizeof(int);
    }

    SM_FileHandle fileHandle;

    // Crear un archivo de página con el nombre de la tabla
    RC result;
    if ((result = createPageFile(name)) != RC_OK) return result;

    // Abrir el archivo recién creado
    if ((result = openPageFile(name, &fileHandle)) != RC_OK) return result;

    // Escribir el esquema en la primera ubicación del archivo
    if ((result = writeBlock(0, &fileHandle, data)) != RC_OK) return result;

    // Cerrar el archivo después de escribir
    if ((result = closePageFile(&fileHandle)) != RC_OK) return result;

    return RC_OK;
}




// This function opens the table with table name "name"
extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pageHandle;    
	
	int attributeCount, k;
	
	// Setting table's meta data to our custom record manager meta data structure
	rel->mgmtData = recordManager;
	// Setting the table's name
	rel->name = name;
    
	// Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);
	
	// Setting the initial pointer (0th location) if the record manager's page data
	pageHandle = (char*) recordManager->pageHandle.data;
	
	// Retrieving total number of tuples from the page file
	recordManager->tuplesCount= *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);

	// Getting free page from the page file
	recordManager->freePage= *(int*) pageHandle;
    	pageHandle = pageHandle + sizeof(int);
	
	// Getting the number of attributes from the page file
    	attributeCount = *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);
 	
	Schema *schema;

	// Allocating memory space to 'schema'
	schema = (Schema*) malloc(sizeof(Schema));
    
	// Setting schema's parameters
	schema->numAttr = attributeCount;
	schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
	schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

	// Allocate memory space for storing attribute name for each attribute
	for(k = 0; k < attributeCount; k++)
		schema->attrNames[k]= (char*) malloc(ATTRIBUTE_SIZE);
      
	for(k = 0; k < schema->numAttr; k++)
    	{
		// Setting attribute name
		strncpy(schema->attrNames[k], pageHandle, ATTRIBUTE_SIZE);
		pageHandle = pageHandle + ATTRIBUTE_SIZE;
	   
		// Setting data type of attribute
		schema->dataTypes[k]= *(int*) pageHandle;
		pageHandle = pageHandle + sizeof(int);

		// Setting length of datatype (length of STRING) of the attribute
		schema->typeLength[k]= *(int*)pageHandle;
		pageHandle = pageHandle + sizeof(int);
	}
	
	// Setting newly created schema to the table's schema
	rel->schema = schema;	

	// Unpinning the page i.e. removing it from Buffer Pool using BUffer Manager
	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

	// Write the page back to disk using BUffer Manger
	forcePage(&recordManager->bufferPool, &recordManager->pageHandle);

	return RC_OK;
}   

extern RC closeTable (RM_TableData *rel)
{
	// Storing the Table's meta data
	// Shutting down Buffer Pool	
	shutdownBufferPool(&((RecordManager*)rel->mgmtData)->bufferPool);
	rel->mgmtData = NULL;
	return RC_OK;
}


// Deletes the table with the specified name from memory.
extern RC deleteTable (char *name)
{
    // Validate that the table name is not null.
    if (name == NULL) {
        return RC_ERROR;
    }

    // Attempt to remove the page file associated with the table.
    RC result = destroyPageFile(name);

    // Check if the destruction of the page file was successful.
    if (result != RC_OK) {
        // Handle the error or return an error code if the file deletion fails.
        return result;
    }

    return RC_OK; // Return success if the table is successfully deleted.
}



// This function returns the number of tuples (records) in the table referenced by "rel"
extern int getNumTuples (RM_TableData *rel)
{
	    if (rel == NULL) {
        // Handle the error, e.g., return an error code or log an error
        return RC_ERROR; 
    }
	// Accessing our data structure's tuplesCount and returning it
	RecordManager *recordManager = rel->mgmtData;
	return recordManager->tuplesCount;
}



// ******** RECORD FUNCTIONS ******** //

// This function inserts a new record in the table referenced by "rel" and updates the 'record' parameter with the Record ID of he newly inserted record
extern RC insertRecord (RM_TableData *rel, Record *record)
{

	    // Validate the input parameters for null pointers.
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_ERROR;
    }
	// Retrieving our meta data stored in the table
	RecordManager *recordManager = rel->mgmtData;	
	
	// Setting the Record ID for this record
	RID *recordID = &record->id; 
	
	char *data, *slotPointer;
	
	// Getting the size in bytes needed to store on record for the given schema
	int recordSize = getRecordSize(rel->schema);
	
	// Setting first free page to the current page
	recordID->page = recordManager->freePage;

	// Pinning page i.e. telling Buffer Manager that we are using this page
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
	
	// Setting the data to initial position of record's data
	data = recordManager->pageHandle.data;
	
	// Getting a free slot using our custom function
	recordID->slot = findFreeSlot(data, recordSize);

	while(recordID->slot == -1)
	{
		// If the pinned page doesn't have a free slot then unpin that page
		unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);	
		
		// Incrementing page
		recordID->page++;
		
		// Bring the new page into the BUffer Pool using Buffer Manager
		pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
		
		// Setting the data to initial position of record's data		
		data = recordManager->pageHandle.data;

		// Again checking for a free slot using our custom function
		recordID->slot = findFreeSlot(data, recordSize);
	}
	
	slotPointer = data;
	
	// Mark page dirty to notify that this page was modified
	markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
	
	// Calculation slot starting position
	slotPointer = slotPointer + (recordID->slot * recordSize);

	// Appending '+' as tombstone to indicate this is a new record and should be removed if space is lesss
	*slotPointer = '+';

	// Copy the record's data to the memory location pointed by slotPointer
	memcpy(++slotPointer, record->data + 1, recordSize - 1);

	// Unpinning a page i.e. removing a page from the BUffer Pool
	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
	
	// Incrementing count of tuples
	recordManager->tuplesCount++;
	
	// Pinback the page	
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);

	return RC_OK;
}

// Inserta un nuevo registro en la tabla y actualiza el parámetro 'record' con el ID del registro insertado.


extern RC deleteRecord (RM_TableData *rel, RID id)
{
    // Validate the input table data to ensure it's not null and contains valid metadata.
    if (rel == NULL || rel->mgmtData == NULL) {
        return RC_ERROR;
    }

    // Retrieve the record manager from the table's metadata.
    RecordManager *recordManager = rel->mgmtData;

    // Pin the page containing the record to be deleted.
    RC pinStatus = pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);
    if (pinStatus != RC_OK) {
        return pinStatus; // Return the error if the pin operation fails.
    }

    // Mark the page as a candidate for storing new records, updating the free page tracker.
    recordManager->freePage = id.page;

    // Calculate the starting position of the record in the page buffer.
    int recordSize = getRecordSize(rel->schema);
    char *targetRecordLocation = recordManager->pageHandle.data + (id.slot * recordSize);

    // Use the tombstone mechanism to mark the record as deleted.
    *targetRecordLocation = '-';

    // Mark the page as dirty to indicate that its contents have been modified.
    RC markDirtyStatus = markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
    if (markDirtyStatus != RC_OK) {
        unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
        return markDirtyStatus;
    }

    // Unpin the page as the deletion process is complete.
    RC unpinStatus = unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
    if (unpinStatus != RC_OK) {
        return unpinStatus;
    }

    return RC_OK; // Return success after successful deletion.
}




extern RC updateRecord (RM_TableData *rel, Record *record){
    // Validate the input parameters to ensure they are not null.
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_ERROR;
    }

    // Extract the record manager from the table's metadata.
    RecordManager *recordManager = rel->mgmtData;

    // Attempt to pin the page containing the record to be updated.
    RC pinStatus = pinPage(&recordManager->bufferPool, &recordManager->pageHandle, record->id.page);
    if (pinStatus != RC_OK) {
        return pinStatus; // Propagate the error from pinPage if it fails.
    }

    // Determine the size of the records according to the schema.
    int recordSize = getRecordSize(rel->schema);

    // Calculate the location in the page buffer where the record data starts.
    char *targetRecordLocation = recordManager->pageHandle.data + (record->id.slot * recordSize);

    // Mark the record as valid (not empty) using the tombstone mechanism.
    *targetRecordLocation = '+';

    // Copy the new data into the record's location in the page, skipping the tombstone character.
    memcpy(targetRecordLocation + 1, record->data + 1, recordSize - 1);

    // Mark the page as dirty because its contents have been modified.
    RC markDirtyStatus = markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
    if (markDirtyStatus != RC_OK) {
        unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
        return markDirtyStatus; // Return the error if marking the page dirty fails.
    }

    // Unpin the page as the update is complete and the page is no longer immediately needed.
    RC unpinStatus = unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
    if (unpinStatus != RC_OK) {
        return unpinStatus;
    }

    return RC_OK; // Return success after a successful update.
}


// This function retrieves a record having Record ID "id" in the table referenced by "rel".
// The result record is stored in the location referenced by "record"
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{

	// Validate input parameters to prevent dereferencing null pointers.
    if (rel == NULL || rel->mgmtData == NULL || record == NULL) {
        return RC_ERROR;
    }
	// Retrieving our meta data stored in the table
	RecordManager *recordManager = rel->mgmtData;
	
	// Pinning the page which has the record we want to retreive
	// Load the page containing the desired record into the buffer pool.
    RC pinStatus = pinPage(&recordManager->bufferPool, &recordManager->pageHandle, id.page);
    if (pinStatus != RC_OK) {
        return pinStatus; // Return the error from pinPage if it fails.
    }

	// Getting the size of the record
	int recordSize = getRecordSize(rel->schema);
	char *dataPointer = recordManager->pageHandle.data + (id.slot * recordSize);
	
	if(*dataPointer != '+')
	{
		unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
		// Return error if no matching record for Record ID 'id' is found in the table
		return RC_ERROR;
	}
	
	// Setting the Record ID
	record->id = id;

		// Setting the pointer to data field of 'record' so that we can copy the data of the record
	char *data = record->data;

		// Copy data using C's function memcpy(...)
	memcpy(++data, dataPointer + 1, recordSize - 1);
	

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);

	return RC_OK;
}


// ******** SCAN FUNCTIONS ******** //

// This function scans all the records using the condition


// Initializes a scan operation on a table with a given condition.
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	// Validate input parameters to prevent dereferencing null pointers
	if (rel == NULL || scan == NULL) {
		return RC_ERROR;
	}

	// Ensure the condition for scanning is provided
	if (cond == NULL) {
		return RC_ERROR;
	}

	// Attempt to open the table for scanning
	if (openTable(rel, "ScanTable") != RC_OK) {
		return RC_ERROR;
	}

	// Allocate memory for the scan manager
	RecordManager *scanManager = (RecordManager*) malloc(sizeof(RecordManager));
	if (scanManager == NULL) {
		return RC_ERROR;
	}

	// Initialize the scan manager's metadata
	scan->mgmtData = scanManager;
	scanManager->recordID.page = 1; // Start from the first page
	scanManager->recordID.slot = 0; // Start from the first slot
	scanManager->scanCount = 0; // No records scanned at initialization
	scanManager->condition = cond; // Set the scan condition

	// Link the scan manager to the table's management data
	RecordManager *tableManager = rel->mgmtData;
	if (tableManager == NULL) {
		free(scanManager); // Cleanup allocated memory
		return RC_ERROR;
	}

	// Initialize table-specific metadata
	tableManager->tuplesCount = ATTRIBUTE_SIZE;
	scan->rel = rel; // Set the scan's target table

	return RC_OK;
}


// Retrieves the next record in the scan that satisfies the given condition.
extern RC next (RM_ScanHandle *scan, Record *record)
{
    // Ensure the scan handle and its management data are not null to avoid segmentation faults.
    if (scan == NULL || scan->mgmtData == NULL) {
        return RC_ERROR;
    }

    RecordManager *scanManager = scan->mgmtData;

    // Ensure the related table data and its management data are valid.
    if (scan->rel == NULL || scan->rel->mgmtData == NULL) {
        return RC_ERROR;
    }
    RecordManager *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;

    // The scan condition must be present to proceed with scanning.
    if (scanManager->condition == NULL) {
        return RC_ERROR;
    }

    // Calculate the size of each record based on the schema and the total number of slots per page.
    int recordSize = getRecordSize(schema);
    int totalSlots = PAGE_SIZE / recordSize;
    int tuplesCount = tableManager->tuplesCount;

    // If no tuples exist in the table, end the scan.
    if (tuplesCount == 0) {
        return RC_RM_NO_MORE_TUPLES;
    }

    // Allocate memory for storing the result of the condition evaluation.
    Value *result = (Value *) malloc(sizeof(Value));
    if (result == NULL) {
        // In case of memory allocation failure, return an error.
        return RC_ERROR;
    }

    // Iterate over tuples to find the one that satisfies the condition.
    while (scanManager->scanCount < tuplesCount) {
        // Update the scan position (page and slot) for the next record.
        updateScanPosition(scanManager, totalSlots);

        // Pin the page to load it into the buffer pool.
        if (pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page) != RC_OK) {
            // Clean up allocated memory and return an error if pinning the page fails.
            free(result);
            return RC_ERROR;
        }

        // Calculate the pointer to the data in the buffer pool.
        char *data = scanManager->pageHandle.data + (scanManager->recordID.slot * recordSize);

        // Prepare the record structure with the data from the buffer pool.
        prepareRecordFromData(record, data, recordSize, scanManager->recordID);

        // Evaluate the current record against the scan condition.
        evalExpr(record, schema, scanManager->condition, &result);

        // Check if the record meets the condition.
        if (result->v.boolV) {
            // Record satisfies condition; unpin the page and return success.
            unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
            free(result);
            return RC_OK;
        }

        // Record did not satisfy condition; unpin the page and continue scanning.
        unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
        scanManager->scanCount++;
    }

    // Free the memory for the result and reset the scan manager as no more tuples satisfy the condition.
    free(result);
    resetScanManager(scanManager);
    return RC_RM_NO_MORE_TUPLES;
}

// Helper functions with comments
void updateScanPosition(RecordManager *scanManager, int totalSlots) {
    // Increment the slot. If it exceeds the total, reset to zero and move to the next page.
    scanManager->recordID.slot++;
    if (scanManager->recordID.slot >= totalSlots) {
        scanManager->recordID.slot = 0;
        scanManager->recordID.page++;
    }
}

void prepareRecordFromData(Record *record, char *data, int recordSize, RID id) {
    // Setup the record ID and initialize the data buffer with a tombstone mechanism.
    record->id = id;
    char *dataPointer = record->data;
    *dataPointer = '-';
    memcpy(++dataPointer, data + 1, recordSize - 1);
}

void resetScanManager(RecordManager *scanManager) {
    // Reset the scan manager's position and count for potential future scans.
    scanManager->recordID.page = 1;
    scanManager->recordID.slot = 0;
    scanManager->scanCount = 0;
}



// Safely terminates the scan process.
extern RC closeScan(RM_ScanHandle *scan) {
    // Ensure scan handle is not null before proceeding.
    if (scan == NULL) {
        // Log error or handle it appropriately.
        return RC_ERROR;
    }

    // Acquire management data for the scan and its associated record.
    RecordManager *scanManager = scan->mgmtData;
    RecordManager *recordManager = (scan->rel) ? scan->rel->mgmtData : NULL;

    // Validate both management data pointers.
    if (scanManager == NULL || recordManager == NULL) {
        // Log error or handle it accordingly.
        return RC_ERROR; 
    }

    // Perform cleanup if a scan operation is in progress.
    if (scanManager->scanCount > 0) {
        // Release the pinned page from the buffer pool.
        unpinPage(&recordManager->bufferPool, &scanManager->pageHandle);

        // Reset scan parameters to initial values.
        scanManager->scanCount = 0;
        scanManager->recordID.page = 1;
        scanManager->recordID.slot = 0;
    }

    // Clear and free the scan management data.
    scan->mgmtData = NULL;
    free(scanManager);

    // Return success code.
    return RC_OK;
}

// ******** SCHEMA FUNCTIONS ******** //

// Calcula el tamaño total requerido para un registro basado en su esquema.
extern int getRecordSize (Schema *schema){

  // Initialize the total size to 0.
    int totalSize = 0;

    // Verify that the schema pointer is not null.
    if (schema == NULL) {
        return -1; // Return -1 or an appropriate error code.
    }

    // Iterate through each attribute in the schema.
    for (int i = 0; i < schema->numAttr; i++) {
        // Determine the size to add based on the data type of the attribute.
        if (schema->dataTypes[i] == DT_STRING) {
            // Add the predefined length for a string attribute.
            totalSize += schema->typeLength[i];
        } else if (schema->dataTypes[i] == DT_INT) {
            // Add the size of an integer.
            totalSize += sizeof(int);
        } else if (schema->dataTypes[i] == DT_FLOAT) {
            // Add the size of a float.
            totalSize += sizeof(float);
        } else if (schema->dataTypes[i] == DT_BOOL) {
            // Add the size of a boolean.
            totalSize += sizeof(bool);
        } else {
            // Handle unexpected data types if necessary.
        }
    }

    // Return the total size, incremented by 1 to account for any additional metadata or end-of-record marker.
    return totalSize + 1;
}

// This function creates a new schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    // Using calloc to allocate and initialize schema memory
    Schema *schema = (Schema *) calloc(1, sizeof(Schema));

    if (schema == NULL) {
        // Handling memory allocation failure
        return NULL;
    }

    // Assigning schema properties
    schema->numAttr = numAttr; // Total number of attributes
    schema->attrNames = attrNames; // Array of attribute names
    schema->dataTypes = dataTypes; // Array of data types for each attribute
    schema->typeLength = typeLength; // Array of type lengths (relevant for variable-length types like strings)
    schema->keySize = keySize; // Number of key attributes
    schema->keyAttrs = keys; // Array of indices of key attributes

    return schema; 
}

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema (Schema *schema){
    if (schema == NULL){
        // Return a specific error code or handle the null pointer scenario as needed.
        return RC_ERROR;
    }

	// De-allocating memory space occupied by 'schema'
	free(schema);
	return RC_OK;
}


// ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //


// Initializes and allocates memory for a new record based on the given schema.
extern RC createRecord (Record **record, Schema *schema)
{
    // Validate the input schema to ensure it's not null.
    if (schema == NULL) {
        return RC_ERROR;
    }

    // Allocate memory for the record structure.
    Record *newRecord = (Record *) malloc(sizeof(Record));
    if (newRecord == NULL) {
        // Memory allocation failed. Return an error code accordingly.
        return RC_ERROR;
    }

    // Determine the size required for the record's data based on the schema.
    int sizeRequiredForData = getRecordSize(schema);

    // Allocate memory for the record's data.
    newRecord->data = (char *) malloc(sizeRequiredForData);
    if (newRecord->data == NULL) {
        // In case of allocation failure for data, clean up already allocated record memory.
        free(newRecord);
        return RC_ERROR;
    }

    // Initialize the record ID to -1 indicating it's new and not yet stored in any page or slot.
    newRecord->id.page = -1;
    newRecord->id.slot = -1;

    // Set up the record's data starting with a tombstone marker followed by a null character.
    newRecord->data[0] = '-'; // Tombstone marker to indicate the record status.
    newRecord->data[1] = '\0'; // Null character to end the string.

    // Assign the newly created record to the output parameter.
    *record = newRecord;

    // Return RC_OK to indicate successful record creation.
    return RC_OK;
}



// This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;

	// Iterating through all the attributes in the schema
	for(i = 0; i < attrNum; i++)
	{
		// Switch depending on DATA TYPE of the ATTRIBUTE
		switch (schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				*result = *result + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}

// Safely deallocates the memory allocated for a record.
extern RC freeRecord (Record *record)
{
    // Check if the record pointer is valid. This is crucial to prevent freeing a null pointer.
    if (record == NULL) {
        // If the record is null, there's nothing to free. An error code is returned
        return RC_ERROR; 
    }
    // Free the memory occupied by the record.
    free(record);

    // Return RC_OK to indicate successful deallocation.
    return RC_OK;
}


// This function retrieves an attribute from the given record in the specified schema
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Allocating memory space for the Value data structure where the attribute values will be stored
	Value *attribute = (Value*) malloc(sizeof(Value));

	// Getting the starting position of record's data in memory
	char *dataPointer = record->data;
	
	// Adding offset to the starting position
	dataPointer = dataPointer + offset;

	// If attrNum = 1
	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
	
	// Retrieve attribute's value depending on attribute's data type
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
     			// Getting attribute value from an attribute of type STRING
			int length = schema->typeLength[attrNum];
			// Allocate space for string hving size - 'length'
			attribute->v.stringV = (char *) malloc(length + 1);

			// Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C
			strncpy(attribute->v.stringV, dataPointer, length);
			attribute->v.stringV[length] = '\0';
			attribute->dt = DT_STRING;
      			break;
		}

		case DT_INT:
		{
			// Getting attribute value from an attribute of type INTEGER
			int value = 0;
			memcpy(&value, dataPointer, sizeof(int));
			attribute->v.intV = value;
			attribute->dt = DT_INT;
      			break;
		}
    
		case DT_FLOAT:
		{
			// Getting attribute value from an attribute of type FLOAT
	  		float value;
	  		memcpy(&value, dataPointer, sizeof(float));
	  		attribute->v.floatV = value;
			attribute->dt = DT_FLOAT;
			break;
		}

		case DT_BOOL:
		{
			// Getting attribute value from an attribute of type BOOLEAN
			bool value;
			memcpy(&value,dataPointer, sizeof(bool));
			attribute->v.boolV = value;
			attribute->dt = DT_BOOL;
      			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}

	*value = attribute;
	return RC_OK;
}

// Updates the specified attribute in the record according to the given schema.
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    // Validate input parameters for null pointers.
    if (record == NULL || schema == NULL || value == NULL) {
        return RC_ERROR;
    }

    // Ensure the attribute number is within the valid range.
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_ERROR;
    }

    // Calculate the offset for the attribute in the record.
    int offset;
    RC status = attrOffset(schema, attrNum, &offset);
    if (status != RC_OK) {
        return status; // Propagate the error from attrOffset.
    }

    // Pointer to the location in the record data where the attribute value should be set.
    char *dataPointer = record->data + offset;

    // Ensure the data type of the value matches the schema.
    if (schema->dataTypes[attrNum] != value->dt) {
        return RC_ERROR;
    }

    // Set the attribute value based on its data type.
    switch (value->dt) {
        case DT_STRING: {
            int length = schema->typeLength[attrNum];
            strncpy(dataPointer, value->v.stringV, length);
            break;
        }
        case DT_INT: {
            memcpy(dataPointer, &value->v.intV, sizeof(int));
            break;
        }
        case DT_FLOAT: {
            memcpy(dataPointer, &value->v.floatV, sizeof(float));
            break;
        }
        case DT_BOOL: {
            memcpy(dataPointer, &value->v.boolV, sizeof(bool));
            break;
        }
        default:
            return RC_ERROR; // Or another appropriate error code.
    }
    return RC_OK;
}
