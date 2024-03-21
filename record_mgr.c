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

#pragma region Table and Manager


// Database Management Functions
RC initRecordManager(void *mgmtData) {

    initStorageManager();
    printf("Record Manager and Storage manager init sucessfully\n");
    
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

#define MAX_NUMBER_OF_PAGES 100000 // Define tu máximo número de páginas
#define ATTRIBUTE_SIZE 500       // Define el tamaño máximo para el nombre del atributo

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

extern RC deleteTable(char *name) {
    // Asegurarse de que el buffer pool para esta tabla esté correctamente liberado
    if (recordManager != NULL) {
        RC status;

        // Primero, cerrar el buffer pool asociado con esta tabla
        status = shutdownBufferPool(&recordManager->bufferPool);
        if (status != RC_OK) {
            return status;
        }

        // Liberar la memoria asignada a la estructura RecordManager
        free(recordManager);
        recordManager = NULL; // Prevenir el uso de punteros colgantes
        
    }

    // Finalmente, destruir el archivo de la tabla
    return destroyPageFile(name);
}


extern RC openTable(RM_TableData *rel, char *name) {
    // Asegurar que rel no es NULL
    if (rel == NULL) {
        return RC_ERROR;
    }

    // Asignar memoria para el esquema y datos de gestión (si es necesario)
    rel->schema = (Schema *)calloc(1,sizeof(Schema));
    if (rel->schema == NULL) {
        return RC_ERROR;
    }

    // Aquí debes cargar el esquema desde el archivo de la tabla
    // Esto depende de cómo hayas implementado la serialización del esquema en createTable

    // Inicializar el buffer pool para esta tabla
    rel->mgmtData = calloc(1, sizeof(BM_BufferPool));
    if (rel->mgmtData == NULL) {
        free(rel->schema);
        return RC_ERROR;
    }

    initBufferPool((BM_BufferPool *)rel->mgmtData, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);

    rel->name = strdup(name);  // Asegurarse de liberar esto en closeTable
    return RC_OK;
}
extern RC closeTable(RM_TableData *rel) {
    // Asegurar que rel no es NULL
    if (rel == NULL) {
        return RC_ERROR;
    }

    // Cerrar el buffer pool asociado con la tabla
    shutdownBufferPool((BM_BufferPool *)rel->mgmtData);
    free(rel->mgmtData);

    // Liberar el esquema
    freeSchema(rel->schema);

    // Liberar el nombre de la tabla
    free(rel->name);
    
    return RC_OK;
}



int getNumTuples(RM_TableData *rel) {
    return RC_OK; // You may need to change this to a relevant return type, as this function returns an int.
}

#pragma endregion 

#pragma region Handling Records in the Table

extern RC insertRecord(RM_TableData *rel, Record *record) {
    // Validate input
    if (rel == NULL || record == NULL) {
        return RC_FILE_NOT_FOUND;
    }
 
    // Cast mgmtData to RecordManager for easier access
    RecordManager *rm = (RecordManager *)rel->mgmtData;

    // Prepare variables
    BM_PageHandle *pageHandle = MAKE_PAGE_HANDLE();
    char *data;
    int recordSize = getRecordSize(rel->schema);
    int numSlots = PAGE_SIZE / recordSize; // Assuming each record fits in a slot and PAGE_SIZE is a constant
 
    // Set to the first available page
    RID *rid = &record->id;
    
    rid->page = rm->freePage; // linea del error

    // Iterate through pages to find a free slot
    bool isRecordInserted = false;
   
    while (!isRecordInserted) {
        CHECK(pinPage(&rm->bufferPool, pageHandle, rid->page));
        data = pageHandle->data;

        for (int slot = 0; slot < numSlots; slot++) {
            if (data[slot * recordSize] != '+') { // Assuming '+' denotes a filled slot
                rid->slot = slot;
                data += slot * recordSize;

                // Marking the page as dirty and updating record info
                data[0] = '+'; // Mark as filled
                memcpy(data + 1, record->data + 1, recordSize - 1);
                markDirty(&rm->bufferPool, pageHandle);
                CHECK(unpinPage(&rm->bufferPool, pageHandle));

                rm->tuplesCount++; // Update tuples count
                isRecordInserted = true;
                break;
            }
        }

        if (!isRecordInserted) {
            // No free slot in the current page
            CHECK(unpinPage(&rm->bufferPool, pageHandle));
            rid->page++; // Move to the next page
        }
    }
 
    free(pageHandle);
    return RC_OK;
}





RC deleteRecord(RM_TableData *rel, RID id) {
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    return RC_OK;
}

extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // Access the table's management data
    RecordManager *rm = rel->mgmtData;

    // Pin the page containing the desired record
    RC rc = pinPage(&rm->bufferPool, &rm->pageHandle, id.page);
    if (rc != RC_OK) {
        return rc; // Return error if pinning fails
    }

    // Calculate the start position of the record in the page
    int sizeOfRecord = getRecordSize(rel->schema);
    char *recordStart = rm->pageHandle.data + (id.slot * sizeOfRecord);

    // Check if the record is valid (assuming '+' indicates validity)
    if (*recordStart != '+') {
        unpinPage(&rm->bufferPool, &rm->pageHandle); // Release the page
        return RC_FILE_NOT_FOUND; // No record found at the given RID
    }

    // Assign Record ID to the retrieved record
    record->id = id;

    // Copy the record's content from the page to the output parameter
    memcpy(record->data, recordStart + 1, sizeOfRecord - 1);

    // Release the page as it's no longer needed in memory
    rc = unpinPage(&rm->bufferPool, &rm->pageHandle);
    return rc; // Return success or error code from unpinning
}

#pragma endregion


#pragma region Scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
    return RC_OK;
}

RC closeScan(RM_ScanHandle *scan) {
    return RC_OK;
}
#pragma endregion


#pragma region Schema Handling Functions
// This function computes the total size of a record defined by the given schema
extern int getRecordSize(Schema *schema) {
    int totalSize = 0; // Initializing total record size

    // Loop through each attribute defined in the schema
    for (int attrIndex = 0; attrIndex < schema->numAttr; attrIndex++) {
        // Determine the size contribution of each attribute based on its type
        DataType type = schema->dataTypes[attrIndex];
        if (type == DT_STRING) {
            // Add length specified for STRING type attributes
            totalSize += schema->typeLength[attrIndex];
        } else if (type == DT_INT) {
            // Increment size by the standard size of an INTEGER
            totalSize += sizeof(int);
        } else if (type == DT_FLOAT) {
            // Increment size by the standard size of a FLOAT
            totalSize += sizeof(float);
        } else if (type == DT_BOOL) {
            // Increment size by the standard size of a BOOLEAN
            totalSize += sizeof(bool);
        }
    }
    return totalSize + 1; // Adding 1 as per original functionality
}


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


extern RC freeSchema(Schema *schema) {
    if (schema != NULL) {
        // Free attribute names
        if (schema->attrNames != NULL) {
            for (int i = 0; i < schema->numAttr; i++) {
                if (schema->attrNames[i] != NULL) {
                    free(schema->attrNames[i]); // Free each individual attribute name
                }
            }
            free(schema->attrNames); // Free the array of attribute names
        }

        // Free data types array
        if (schema->dataTypes != NULL) {
            free(schema->dataTypes);
        }

        // Free type lengths array
        if (schema->typeLength != NULL) {
            free(schema->typeLength);
        }

        // Free key attributes array
        if (schema->keyAttrs != NULL) {
            free(schema->keyAttrs);
        }

        // Finally, free the schema structure itself
        free(schema);
    }
    return RC_OK;
}

#pragma endregion

#pragma region Record and Attribute Value Functions
RC createRecord(Record **record, Schema *schema) {
    
    Record *tempRecord = (Record*) malloc(sizeof(Record));
    if (tempRecord == NULL) {
        return RC_FILE_NOT_FOUND;
            }

    int recordSize = getRecordSize(schema);
    tempRecord->data = (char*) calloc(recordSize, sizeof(char));
    if (tempRecord->data == NULL) {
        free(tempRecord);
        return RC_FILE_NOT_FOUND;
    }

    *record = tempRecord;
    return RC_OK;
}

  // modifiying
extern RC freeRecord(Record *record) {
    if (record != NULL) {
        // Ensure that any dynamically allocated fields within the record are freed first
        // Assuming 'data' is dynamically allocated within the record, if applicable
        if (record->data != NULL) {
            free(record->data);
        }

        // Freeing the record structure itself
        free(record);
    } else {
        // Handle the case where the record is NULL
        return RC_ERROR; // Or an appropriate error code for a null pointer
    }

    return RC_OK;
}



extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if (record == NULL || schema == NULL) {
        return RC_ERROR; // Or some other appropriate error code
    }

    // Calculate attribute offset
    int offset = 0, attrSize;
    attrOffset(schema, attrNum, &offset);

    // Allocate memory for Value structure
    *value = (Value *)malloc(sizeof(Value));
    if (*value == NULL) {
        return RC_ERROR;
    }

    // Pointer to the attribute in record data
    char *attrData = record->data + offset;
    DataType attrType = schema->dataTypes[attrNum];

    // Handling different data types
    switch (attrType) {
        case DT_STRING:
            attrSize = schema->typeLength[attrNum];
            (*value)->v.stringV = (char *)malloc(attrSize + 1);
            if ((*value)->v.stringV == NULL) {
                free(*value);
                return RC_READ_NON_EXISTING_PAGE;
            }
            strncpy((*value)->v.stringV, attrData, attrSize);
            (*value)->v.stringV[attrSize] = '\0';
            break;

        case DT_INT:
            memcpy(&((*value)->v.intV), attrData, sizeof(int));
            break;

        case DT_FLOAT:
            memcpy(&((*value)->v.floatV), attrData, sizeof(float));
            break;

        case DT_BOOL:
            memcpy(&((*value)->v.boolV), attrData, sizeof(bool));
            break;

        default:
            free(*value);
            return RC_RM_UNKOWN_DATATYPE;
    }

    (*value)->dt = attrType;
    return RC_OK;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if (!record || !schema || !value) {
        return RC_FILE_NOT_FOUND;
    }

    // Calcular el desplazamiento del atributo en los datos del registro
    int offset = 0;
    attrOffset(schema, attrNum, &offset);
    char *attrLocation = record->data + offset;

    // Actualizar el atributo basado en su tipo
    switch (schema->dataTypes[attrNum]) {
        case DT_STRING:
            // Limpiar y copiar el nuevo valor de la cadena
            memset(attrLocation, 0, schema->typeLength[attrNum]);
            strncpy(attrLocation, value->v.stringV, schema->typeLength[attrNum]);
            break;

        case DT_INT:
            // Actualizar el valor entero
            *(int *)(attrLocation) = value->v.intV;
            break;

        case DT_FLOAT:
            // Actualizar el valor flotante
            *(float *)(attrLocation) = value->v.floatV;
            break;

        case DT_BOOL:
            // Actualizar el valor booleano
            *(bool *)(attrLocation) = value->v.boolV;
            break;

        default:
            // Manejar tipos de datos no reconocidos
            return RC_RM_UNKOWN_DATATYPE;
    }

    return RC_OK;
}




#pragma endregion