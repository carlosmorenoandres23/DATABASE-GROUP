### Assignment 3  ###

## ---> Record Manager <--- ##

## Group Members ##
1. Carlos Moreno Andres (A20553013)
2. Pablo Gomez (A20552996)
3. Anshuman Raturi (A20531913)
4. Shubham Kapopara (A20561911)

## Code running instructions

1. Navigate to the project's root directory (assign3) using the Terminal.

2. Use the `ls` command to list the files and ensure that you are in the correct directory.

3. Execute `make clean` to remove any previously compiled `.o` files.

4. Run `make` to compile all project files, including `test_assign3_1.c`.

5. Start the script by running `./recordmgr`.

6. For testing expression-related files, use `make test_expr` to compile, and then run `./test_expr`.

## Solution Description ##

--> The main goals of this Record Manager implementation are to handle deleted records with the Tombstone mechanism, minimize variable usage, and manage memory efficiently.

### 1. Managing Tables and Records ###

### `initRecordManager(...)`

--> This function initializes the record manager, leveraging `initStorageManager(...)` to set up storage.
--> It ensures proper memory allocation.

### `shutdownRecordManager(...)`

-->  Properly shuts down the record manager, releasing all resources.
--> Guarantees memory deallocation.
--> Sets the recordManager data structure pointer to NULL and employs `free()` for memory release.

### `createTable(...)`

--> Opens a table with a specified name.
--> Initializes the Buffer Pool using `initBufferPool(...)` and sets table attributes like name, datatype, and size.
--> Creates a page file, writes the block containing the table, and then closes the page file.

### `openTable(...)`

--> Creates a table with the specified name and schema.

### `closeTable(...)`

--> Closes the table referred to by the parameter 'rel'.
--> Ensures changes to the table in the page file are written by invoking Buffer Manager's `shutdownBufferPool(...)` before closing the buffer pool.

### `deleteTable(...)`

--> Deletes the table specified by the name.
--> Utilizes Storage Manager's `destroyPageFile(...)` to delete the page from disk and free memory space.

### `getNumTuples(...)`

--> Retrieves the number of tuples in the table referred to by the parameter 'rel'.


### 2. Recording Functions ###

### `insertRecord(...)` 
--> The Insert Record function adds a new entry to the table and updates the Record ID provided in the `record` parameter. 
--> It allocates space for the new record, marks it with a '+' indicating it's new, and sets the page status to dirty. 
--> Using `memcpy()`, it copies the record data to the new entry and then unpins the page.

### `deleteRecord(...)`

--> The Delete Record function removes a record from the table based on the given Record ID. It identifies the page containing the record, marks it with a '-' to signify deletion, sets the page as dirty, and unpins the page. 
--> The Page ID of the page containing the record to be removed is assigned to the table's `freePage` metadata.

### `updateRecord(...)`

--> The Update Record function modifies a specific record identified by the 'record' parameter in the table. 
--> It locates the record page, updates the Record ID, navigates to the data location, copies the modified data, marks the page as dirty, and then releases the pin.

### `getRecord(...)` 

--> The Get Record function retrieves a record from the table based on the provided Record ID. It unpins the page, copies the data, and sets the Record ID of the provided 'record' argument.

### 3. Scanning Functions Overview ###

### `startScan(...)`

--> This function initiates a scan operation using the provided RM_ScanHandle data structure.
--> It initializes variables relevant to the scan process within a custom data structure.
--> If the scan condition is NULL, it returns an error code (`RC_SCAN_CONDITION_NOT_FOUND`).

### `next(...)`

--> The `next(...)` function retrieves the next tuple that satisfies the scan condition.
--> If the scan condition is NULL, it returns an error code (`RC_SCAN_CONDITION_NOT_FOUND`).
--> If no tuples meet the condition, it returns `RC_RM_NO_MORE_TUPLES`.
--> During operation, it iterates through tuples, pins the page, copies data, and evaluates the test expression.
--> If a tuple meets the condition, it returns `RC_OK`; otherwise, it returns `RC_RM_NO_MORE_TUPLES`.

### `closeScan(...)`

--> This function concludes a scan operation.
--> It checks for incomplete scans based on the scanCount value in the table's metadata.
--> If the scan was incomplete, it unpins the page, resets scan-related variables, and deallocates memory space used by the metadata.

### 5. Working with Attributes in Functions ###

## `createRecord(...)`

--> Introduces a new record into the given schema and assigns it.
--> Allocates memory for the record's data.
--> To signify an empty record, append '-' and '\0' as placeholders.

## `attrOffset(...)`

--> Computes the byte offset between the beginning location and the specified attribute within the record.
--> Stores this value in the 'result' parameter.

## `freeRecord(...)`

--> Releases the memory allocated for the provided record using the `free()` function.

## `getAttr(...)`

--> Retrieves a property from the given record within the defined schema.
--> Records the attribute details in the 'value' parameter.

## `setAttr(...)`

--> Adjusts the value of a record's attribute based on the provided schema.
--> Converts and assigns the data from the 'value' parameter to the attribute's data type and value.

--> Comprehensive functionality for managing tables, records, scans, schemas, and attributes is provided by this Record Manager implementation. In order to maximize system resources, it gives priority to effective memory management. Furthermore, it incorporates a Tombstone mechanism to manage removed entries efficiently, guaranteeing system functionality and data integrity. The database's consistency and structure are maintained while allowing for the appropriate deletion of deleted records thanks to the Tombstone method. This guarantees the system's continued stability and ability to efficiently handle a variety of data management activities.

