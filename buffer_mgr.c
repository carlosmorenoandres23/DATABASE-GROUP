#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include <limits.h>

typedef struct PageFrame {
    SM_PageHandle data; // Actual data of the page
    PageNumber pageNum; // An identification integer given to each page
    int dirtyBit;       // Indicates if the page has been modified
    int fixCount;       // Number of clients using this page
    int hitNum;         // Used by LRU for least recently used page
    int refNum;         // Used by LFU for least frequently used page
} PageFrame;


// "rearIndex" basically stores the count of number of pages read from the disk.
// "rearIndex" is also used by FIFO function to calculate the frontIndex i.e.
int rearIndex = 0;

// "writeCount" counts the number of I/O write to the disk i.e. number of pages writen to the disk
int writeCount = 0;

// Global variables related to buffer pool management
int bufferSize = 0;           // Size of the buffer pool
int numPagesReadCount = 0;    // Count of pages read from disk
int totalDiskWriteCount = 0;  // Count of pages written to disk
int hit = 0;                  // General count incremented for each added page frame
int clockPointer = 0;         // Used by CLOCK algorithm
int lfuPointer = 0;           // Used by LFU algorithm to speed up operations


// Replacement Strategy Functions //

// Writes a page frame's data to disk and updates write count
bool writeBlockToDisk(BM_BufferPool *const bm, PageFrame *pageFrame, int pageFrameIndex)
{
    SM_FileHandle fh;
    RC openStatus, writeStatus;
     
    // Attempt to open the page file associated with the buffer pool 
    openStatus = openPageFile(bm->pageFile, &fh);
    if (openStatus != RC_OK) return false; // Check if the file opened correctly

    writeStatus = writeBlock(pageFrame[pageFrameIndex].pageNum, &fh, pageFrame[pageFrameIndex].data);
    if (writeStatus != RC_OK) return false; // Check if the block was written correctly

    // Attempt to write the page frame's data to the page file on disk
    totalDiskWriteCount++; // Increment the count of disk writes
    return true; // Confirm successful execution of the function
}


bool setNewPageToPageFrame(PageFrame *pageFrame, PageFrame *page, int pageFrameIndex)
{
    // Verify that the pageFrame and page pointers are valid
    if (pageFrame == NULL || page == NULL) return false; // Ensure the pointers are not null

    // Copy the page content and its attributes to the target page frame
    pageFrame[pageFrameIndex].dirtyBit = page->dirtyBit;
    pageFrame[pageFrameIndex].pageNum = page->pageNum;
    pageFrame[pageFrameIndex].data = page->data;
    pageFrame[pageFrameIndex].hitNum = page->hitNum;
    pageFrame[pageFrameIndex].fixCount = page->fixCount;

    return true; // Confirm successful execution of the function
}

void FIFO(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    int currentIndex = numPagesReadCount % bufferSize; // Calculate the current index based on the number of pages read

    // Loop through the buffer pool to find a suitable page frame for replacement
    for (int iter = 0; iter < bufferSize; iter++) {
        if (pageFrame[currentIndex].fixCount == 0) { // Page frame not in use
            if (pageFrame[currentIndex].dirtyBit == 1) { // Check if the page has been modified
                writeBlockToDisk(bm, pageFrame, currentIndex); // Write modified page back to disk
            }
            
            // Libera la memoria de la página actual antes de reemplazarla
            if (pageFrame[currentIndex].data != NULL) {
                free(pageFrame[currentIndex].data);
                pageFrame[currentIndex].data = NULL; // Evitar punteros colgantes
            }

            setNewPageToPageFrame(pageFrame, page, currentIndex); // Set new page to the current page frame
            break; // Exit the loop after setting the new page
        }

        // Move to the next page frame and wrap around if at the end of the buffer
        currentIndex = (currentIndex + 1) % bufferSize;
    }
}

// Implementation of Least Frequently Used (LFU) page replacement algorithm
void LFU(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    int leastFreqIndex = lfuPointer, leastFreqRef = pageFrame[lfuPointer].refNum;

    // Iterate through all page frames to find the least frequently used one
    for (int i = 0; i < bufferSize; i++) {
        int currentIndex = (lfuPointer + i) % bufferSize; // Calculate the current index
        if (pageFrame[currentIndex].fixCount == 0 && pageFrame[currentIndex].refNum < leastFreqRef) {
            leastFreqIndex = currentIndex; // Update the least frequently used index
            leastFreqRef = pageFrame[currentIndex].refNum; // Update the least frequency
        }
    }

    // Replace the least frequently used page frame if it's not in use
    if (pageFrame[leastFreqIndex].dirtyBit == 1) {
        writeBlockToDisk(bm, pageFrame, leastFreqIndex); // Write modified page back to disk
    }

    if (pageFrame[leastFreqIndex].data != NULL) {
    free(pageFrame[leastFreqIndex].data);
    pageFrame[leastFreqIndex].data = NULL; // Evitar punteros colgantes
    }

    setNewPageToPageFrame(pageFrame, page, leastFreqIndex); // Set new page to the least frequently used page frame
    lfuPointer = (leastFreqIndex + 1) % bufferSize; // Update the LFU pointer for next use
}


// Implementation of Least Recently Used (LRU) page replacement algorithm
void LRU(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    int leastHitIndex = -1, leastHitNum = INT_MAX; // Initialize with maximum possible values

    // Loop through the buffer pool to find the least recently used page frame
    for (int i = 0; i < bufferSize; i++) {
        if (pageFrame[i].fixCount == 0 && pageFrame[i].hitNum < leastHitNum) { // Page frame is not in use and has the least hit number
            leastHitIndex = i; // Update the least recently used index
            leastHitNum = pageFrame[i].hitNum; // Update the least hit number
        }
    }

    // Replace the least recently used page frame if found
    if (leastHitIndex != -1) {
        if (pageFrame[leastHitIndex].dirtyBit == 1) {
            writeBlockToDisk(bm, pageFrame, leastHitIndex); // Write modified page back to disk
        }

        // Libera la memoria de la página actual antes de reemplazarla
        if (pageFrame[leastHitIndex].data != NULL) {
        free(pageFrame[leastHitIndex].data);
        pageFrame[leastHitIndex].data = NULL; // Evitar punteros colgantes
        }
        setNewPageToPageFrame(pageFrame, page, leastHitIndex); // Set new page to the least recently used page frame
    }
}

// Implementation of CLOCK page replacement algorithm
void CLOCK(BM_BufferPool *const bm, PageFrame *page) {
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Continuously loop until a suitable page frame is found
    while (true) {
        if (pageFrame[clockPointer].fixCount == 0) { // Check if the current page frame is not in use
            if (pageFrame[clockPointer].dirtyBit == 1) { // Check if the page has been modified
                writeBlockToDisk(bm, pageFrame, clockPointer); // Write modified page back to disk
            }

            // Libera la memoria de la página actual antes de reemplazarla
            if (pageFrame[clockPointer].data != NULL) {
                free(pageFrame[clockPointer].data);
                pageFrame[clockPointer].data = NULL; // Evitar punteros colgantes
            }

            setNewPageToPageFrame(pageFrame, page, clockPointer); // Set new page to the current page frame
            clockPointer = (clockPointer + 1) % bufferSize; // Move the clock pointer to the next page frame
            break; // Exit the loop after setting the new page
        } else {
            pageFrame[clockPointer].hitNum = 0; // Reset the hit number for the current page frame
            clockPointer = (clockPointer + 1) % bufferSize; // Move to the next page frame
        }
    }
}

PageFrame *newPage;
PageFrame *page;


// BUFFER POOL FUNCTIONS //
/*
   This function creates and initializes a buffer pool with numPages page frames.
   pageFileName stores the name of the page file whose pages are being cached in memory.
   strategy represents the page replacement strategy (FIFO, LRU, LFU, CLOCK) that will be used by this buffer pool
   stratData is used to pass parameters if any to the page replacement strategy
*/


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                         const int numPages, ReplacementStrategy strategy,
                         void *stratData)
{   // Assign the page file, number of pages, and strategy to the buffer pool
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bufferSize = numPages;

    // Allocate memory for page frames in the buffer pool
    PageFrame *pageFrames = malloc(sizeof(PageFrame) * numPages);
    // Check if memory allocation was successful
    if (pageFrames == NULL) return RC_ERROR;

    // Initialize all page frames in the buffer pool
    for (int i = 0; i < bufferSize; i++)
    {
        PageFrame *currentPageFrame = &pageFrames[i];
        currentPageFrame->data = NULL;
        currentPageFrame->pageNum = -1;
        currentPageFrame->dirtyBit = 0;
        currentPageFrame->fixCount = 0;
        currentPageFrame->hitNum = 0;        // Reset hit number for replacement strategy
        currentPageFrame->refNum = 0;        // Reset reference number for replacement strategy
    
    }
    // Set the management data for the buffer pool
    bm->mgmtData = pageFrames;
    // Reset counters and pointers used in replacement strategies
    totalDiskWriteCount = clockPointer = lfuPointer = 0;
    return RC_OK;
}


// Shutdown i.e. close the buffer pool, thereby removing all the pages from the memory and freeing up all resources and releasing some memory space.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	// Write all dirty pages (modified pages) back to disk
	forceFlushPool(bm);

	int i;	
	for(i = 0; i < bufferSize; i++)
	{
		// If fixCount != 0, it means that the contents of the page was modified by some client and has not been written back to disk.
		if(pageFrame[i].fixCount != 0)
		{
			return RC_ERROR;
		}
	}

	// Releasing space occupied by the page
	free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}

// Force flush all dirty pages in the buffer pool to disk
RC forceFlushPool(BM_BufferPool *const bm)
{
    // Check if the buffer pool is initialized
	if (bm == NULL) {
			// If the buffer pool pointer is NULL, the buffer pool is not initialized
			return RC_FILE_HANDLE_NOT_INIT;
		}

	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    
    // Check if the page frame is initialized
	if (pageFrame == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

	int i;
	RC rc;
	// Store all dirty pages (modified pages) in memory to page file on disk
	for (i = 0; i < bufferSize; i++)
	{
		if (pageFrame[i].fixCount == 0 && pageFrame[i].dirtyBit == 1)
		{
			SM_FileHandle fh;
			// Opening page file available on disk
			rc = openPageFile(bm->pageFile, &fh);
			if (rc != RC_OK) {
                return rc; // Return the error code if opening the file fails
            }
			// Writing block of data to the page file on disk
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
			// Mark the page not dirty.
			pageFrame[i].dirtyBit = 0;
			// Increase the totalDiskWriteCount which records the number of writes done by the buffer manager.
			totalDiskWriteCount++;
		}
	}
	return RC_OK;
}


// PAGE MANAGEMENT FUNCTIONS //
// This function marks the page as dirty indicating that the data of the page has been modified by the client


// Marks a specific page as 'dirty', indicating it has been modified
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    // Check if the buffer pool or page handle is not initialized
    if (bm == NULL || page == NULL || bm->mgmtData == NULL) {
        return RC_ERROR; // Error code for uninitialized structures
    }

    PageFrame *pageFrames = (PageFrame *)bm->mgmtData;

    // Find the page with the given page number and mark it as dirty
    for (int i = 0; i < bm->numPages; ++i) {
        if (pageFrames[i].pageNum == page->pageNum) {
            pageFrames[i].dirtyBit = 1;
            return RC_OK;
        }
    }

    // If the page is not found, return an error
    return RC_ERROR; // Error code for page not found in buffer
}

// Unpins a page from the buffer pool
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    // Validate the input parameters
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) {
        return RC_ERROR; // Error code for invalid input
    }

    PageFrame *pageFrames = (PageFrame *)bm->mgmtData;
    bool pageFound = false;

    // Iterate through the buffer pool to find and unpin the page
    for (int i = 0; i < bm->numPages; i++) { // Using bm->numPages for the loop
        if (pageFrames[i].pageNum == page->pageNum) {
            if (pageFrames[i].fixCount > 0) {
                pageFrames[i].fixCount--;
            }
            pageFound = true;
            break;// Exit loop once page is found and unpinned
        }
    }
    
     // Return appropriate status based on whether the page was found
    return pageFound ? RC_OK : RC_ERROR; // Return RC_PAGE_NOT_FOUND if page is not found in the buffer
}

// Writes the contents of a specified modified page back to the disk
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
     // Validate input parameters
    if (!bm || !bm->mgmtData || !page) {
        return RC_ERROR; // Error code for invalid inputs
    }

    PageFrame *pageFrames = (PageFrame *)bm->mgmtData;
    SM_FileHandle fileHandle;
    bool pageWritten = false;

   // Iterate through the buffer pool to find the page
    int i = 0;
    while (i < bm->numPages) {
        if (pageFrames[i].pageNum == page->pageNum) {
            // Open the page file
            if (openPageFile(bm->pageFile, &fileHandle) != RC_OK) {
                return RC_FILE_NOT_FOUND; // Error handling for file opening
            }
            

            // Write the page back to disk
            if (writeBlock(pageFrames[i].pageNum, &fileHandle, pageFrames[i].data) != RC_OK) {
                return RC_WRITE_FAILED; // Error handling for writing to disk
            }

            pageFrames[i].dirtyBit = 0; // Clear the dirty bit after writing
            pageWritten = true;
            break; // Exiting loop after writing the page
        }
        i++;
    }

    // Finalize the operation
    if (pageWritten) {
        totalDiskWriteCount++; // Incrementing the disk write count
        return RC_OK;
    }

    return RC_ERROR; // Return error if page not found
}

// This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool.
// If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned.

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Handling the case where the buffer pool is empty
    if (pageFrame[0].pageNum == -1) {
        // Check if buffer pool is empty and this is the first page to be pinned
        // Read page from disk and initialize page frame's content in the buffer pool
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh); // Open the page file corresponding to the buffer pool

        pageFrame[0].data = (SM_PageHandle)malloc(PAGE_SIZE); // Allocate memory for the page's content
        readBlock(pageNum, &fh, pageFrame[0].data);

        pageFrame[0].pageNum = pageNum; // Set the page number and increment fix count
        pageFrame[0].fixCount++;
        numPagesReadCount = hit = 0;
        pageFrame[0].hitNum = hit;
        
        pageFrame[0].refNum = 0; // Initialize the reference number for the page frame

        page->pageNum = pageNum;  // Set the page handle's properties to reflect the pinned page
        page->data = pageFrame[0].data;
        return RC_OK;
    }
    else {
        // Buffer pool has at least one page
        bool isBufferFull = true;
        int i;
        for (i = 0; i < bufferSize; i++) {
            // Check if current page frame is in use (i.e., not -1)
            if (pageFrame[i].pageNum != -1) {
                // Check if page is in memory
                if (pageFrame[i].pageNum == pageNum) {
                    // Increase fixCount as another client is accessing this page
                    pageFrame[i].fixCount++;
                    isBufferFull = false; // Buffer is not full as page is found
                    hit++; // Increment the hit counter for replacement strategy
                    // Update hitNum according to the replacement strategy
                    
                    // Update replacement strategy specific counters
                    if (bm->strategy == RS_LRU)
                        pageFrame[i].hitNum = hit;
                    else if (bm->strategy == RS_CLOCK)
                        pageFrame[i].hitNum = 1;
                    else if (bm->strategy == RS_LFU)
                        pageFrame[i].refNum++;
                    
                    // Set the page handle to the found page
                    page->pageNum = pageNum;
                    page->data = pageFrame[i].data;
                    clockPointer++; // Increment the clock pointer for CLOCK strategy
                    break; // Exit the loop as page is found and handled
                }
            }
            else {
                
                // This section handles the case where a buffer slot is empty
                // Opening the page file associated with the buffer pool
                SM_FileHandle fh;
                openPageFile(bm->pageFile, &fh);

                // Allocating memory for the page's content
                pageFrame[i].data = (SM_PageHandle)malloc(PAGE_SIZE);
                // Reading the specified page from disk into the buffer pool// Reading the specified page from disk into the buffer pool
                readBlock(pageNum, &fh, pageFrame[i].data);
                pageFrame[i].pageNum = pageNum; // Assigning page numberv
                pageFrame[i].fixCount = 1;
                pageFrame[i].refNum = 0; // Initializing reference number
                
                // Incrementing counters for pages read and hits for replacement strategies
                numPagesReadCount++;
                hit++;

                // Updating hit number based on the chosen replacement strategy
                if (bm->strategy == RS_LRU)
                    pageFrame[i].hitNum = hit;
                else if (bm->strategy == RS_CLOCK)
                    pageFrame[i].hitNum = 1;
                // Setting the page handle properties to reflect the newly pinned page    
                page->pageNum = pageNum;
                page->data = pageFrame[i].data;
                // Marking the buffer as not full since a page was successfully added
                isBufferFull = false;
                break; // Exiting the loop as the page has been handled
            }
        }

        if (isBufferFull == true) {
            // Handling the full buffer pool case
            // Allocate memory for a new page frame
            PageFrame *newPage = (PageFrame *)malloc(sizeof(PageFrame)); // reservar memoria si esta llena
            SM_FileHandle fh;
            openPageFile(bm->pageFile, &fh);

            // Allocate memory for the page's content and read the page from disk
            newPage->data = (SM_PageHandle)malloc(PAGE_SIZE);
            readBlock(pageNum, &fh, newPage->data);

            // Initialize the properties of the new page frame
            newPage->pageNum = pageNum;
            newPage->dirtyBit = 0;
            newPage->fixCount = 1;
            newPage->refNum = 0;

            // Update counters for pages read and hits
            numPagesReadCount++;
            hit++;

            // Set hit number based on the buffer pool's replacement strategy
            if (bm->strategy == RS_LRU)
                newPage->hitNum = hit;
            else if (bm->strategy == RS_CLOCK)
                newPage->hitNum = 1;
                
            // Set the page handle to the new page    
            page->pageNum = pageNum;
            page->data = newPage->data;
            
            // Implement the appropriate page replacement strategy
            switch (bm->strategy) {
            case RS_FIFO:
                FIFO(bm, newPage);
                free(newPage); 
                break;
            case RS_LRU:
                LRU(bm, newPage);
                free(newPage); 
                break;
            case RS_CLOCK:
                CLOCK(bm, newPage);
                free(newPage); 
                break;
            case RS_LFU:
                LFU(bm, newPage);
                free(newPage); 
                break;
            case RS_LRU_K:
                break;
            default:
                printf("\n Not implementation of the algorithm");
                break;
            }
        }
        return RC_OK;
    }
}

// STATISTICS FUNCTIONS //


// This function returns an array of page numbers.
PageNumber *getFrameContents(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; // Return NULL if buffer pool or its management data is not initialized
    }

    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    // Allocate memory for an array of page numbers
    PageNumber *frameContents = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);

    // Iterating through all the pages in the buffer pool
    for (int i = 0; i < bm->numPages; i++) {
        // Assign page number or NO_PAGE for empty frames
        frameContents[i] = pageFrame[i].pageNum != -1 ? pageFrame[i].pageNum : NO_PAGE;
    }

    return frameContents; // Return the array of page numbers
}



// This function returns an array of bools, each element represents the dirtyBit of the respective page.
// This function returns an array of bools, each element represents the dirtyBit of the respective page.
bool *getDirtyFlags(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; // Return NULL if buffer pool or its management data is not initialized
    }
    // Allocate memory for dirty flags array
    bool *dirtyFlags = (bool *)malloc(sizeof(bool) * bm->numPages);
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

    // Iterate through all pages in the buffer pool
    for (int i = 0; i < bm->numPages; i++) {
        // Set dirty flag value based on the dirtyBit of the page
        dirtyFlags[i] = pageFrame[i].dirtyBit == 1;
    }

    return dirtyFlags;// Return the array of dirty flags
}

// This function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
int *getFixCounts(BM_BufferPool *const bm) {
    // Check if buffer pool or its management data is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; // Return NULL if buffer pool or its management data is not initialized
    }
    
    // Allocate memory for an array of fix counts
    PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
    int *fixCounts = (int *)malloc(sizeof(int) * bm->numPages);

    // Iterate through all the pages in the buffer pool
    for (int i = 0; i < bm->numPages; i++) {
        fixCounts[i] = pageFrame[i].fixCount != -1 ? pageFrame[i].fixCount : 0;
    }

    return fixCounts;// Return the array of fix counts
}


// Returns the total number of page read operations from disk for the specified buffer pool.
int getNumReadIO(BM_BufferPool *const bm)
{
    // Check if buffer pool or its management data is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; // Return NULL if buffer pool or its management data is not initialized
    }
	 // Incrementing by one as the initial count starts from 0.
	return (numPagesReadCount + 1);
}

// Returns the total number of page write operations to disk for the specified buffer pool.
int getNumWriteIO(BM_BufferPool *const bm)
{       // Check if buffer pool or its management data is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return NULL; // Return NULL if buffer pool or its management data is not initialized
    }
	 // Directly returning the count of pages written to disk.
	return totalDiskWriteCount;
}