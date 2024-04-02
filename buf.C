#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/* Function named allocBuf.
*  Input: frame -> an integer reference. changes to frame will be kept
*  Output: 'status' object
*   - BUFFEREXCEEDED if all buffer frames are pinned
*   - UNIXERR if the call to I/O returned an error
*   - OK else
*  Used By: readPage() and allocPage()
*  Directions:
*    - find and allocate a free frame using clock
*    - Writes dirty page to disk
*    - If we remove a valid page, then remove it from hash table 
*/
const Status BufMgr::allocBuf(int & frame) 
{
    Status status = OK;
    int count = 0;
    // TODO Case: if all buffer frames are pinned, maybe loop through the clock twice, 
    //           return BUFFEREXCEEDED if out of loop. Or use a bool
    while (true){
        count += 1;
        //return BUFFEREXCEEDED if all frames are pinned
        if(count >= numBufs*2){
            status = BUFFEREXCEEDED;
            break;
        }
        //advance clock
        advanceClock();
        //check if it is a valid set
        if(!bufTable[clockHand].valid){
            break;
        }
        //check if the frame is pinned
        if(bufTable[clockHand].pinCnt != 0){
            continue;
        }
        //check if recently used
        if(bufTable[clockHand].refbit){
            //clear the reference bit 
            bufTable[clockHand].refbit = false;
            continue;
        }
        //it is avaliable
        //check if dirty
        if(bufTable[clockHand].dirty){
            //if dirty, write back to disk
            status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,&(bufPool[clockHand]));
            //UNIXERR if the call to the I/O layer returned an error
            if (status != OK){
                status = UNIXERR;
            }
        }
        //remove the page
        hashTable->remove(bufTable[clockHand].file,bufTable[clockHand].pageNo);
        bufTable[clockHand].Clear();
        status = OK;
        break;
    }
    //return the free frame via the frame parameter
    frame = clockHand;
    //return the status
    return status;
}

/**
 * Read a specific page from the buffer pool or loads it from 
 * disk if it's not already present by allocating a new buffer
 * frame using allocBuf.
 * @param file A pointer to the file that contains the page
 * @param PageNo The page number of the desired page within the file
 * @param page A reference to a pointer to a Page object
 * @return Status:
 *              - OK if no errors occurred
 *              - UNIXERR if a Unix error occurred
 *              - BUFFEREXCEEDED if all buffer frames are pinned
 *              - HASHTBLERROR if a hash table error occurred
 *         A pointer to the frame containing the page via the page parameter
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status = OK;
    int frame;
    // Check whether the page is already in the buffer pool
    // Returns HASHNOTFOUND if entry is not found and OK otherwise
    Status lookupStatus = hashTable->lookup(file, PageNo, frame);
    // Case 1: Page is in the buffer pool
    if(lookupStatus=OK){
        //set the appropriate refbit
        bufTable[frame].refbit = true;
        //increment the pinCnt for the page
        bufTable[frame].pinCnt += 1;
        //return a pointer to the frame containing the page via the page parameter.
        page = &bufPool[frame];
    } 
    // Case 2: Page is not in the buffer pool
    else{
        //Call allocBuf() to allocate a buffer frame
        status = allocBuf(frame);
        if(status != OK){
            return status;
        }
        //call the method file->readPage() to read the page from disk into the buffer pool frame
        file->readPage(PageNo,&bufPool[frame]);
        if(status != OK){
            return status;
        }
        //insert the page into the hashtable
        status = hashTable->insert(file,PageNo,frame);
        if(status != OK){
            return status;
        }
        //invoke Set() on the frame to set it up properly
        bufTable[frame].Set(file,PageNo);
        //Return a pointer to the frame containing the page via the page parameter
        page = &bufPool[frame];
    }
    //return the status
    return status;
}


/**
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, 
 * sets the dirty bit.  Returns OK if no errors occurred, 
 * @param file A pointer to the file that contains the page
 * @param PageNo the page number of the desired page within the file
 * @param dirty says wheter the file will be written or not
 * @return Status:
 *              - OK if no errors occurred
 *              - HASHNOTFOUND if the page is not in the buffer pool hash table,
 *              - PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty){
    Status status;
    int frameNo; // frame number of page in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);

    if (status == HASHNOTFOUND){
        return status;
    }

    if (bufTable[frameNo].pinCnt == 0){
        return PAGENOTPINNED;
    }

    if (dirty){
        bufTable[frameNo].dirty = true;
    }

    bufTable[frameNo].pinCnt -= 1;
    return OK;

} 


/**
 * Allocates a new page in the specified file.
 * @param file A pointer to the file where the new page will be added
 * @param pageNo An integer that will be updated with the number of the newly allocated page
 * @param page A reference to a pointer to a Page object
 * @return Status:
 *              - OK if no errors occurred
 *              - UNIXERR if a Unix error occurred
 *              - BUFFEREXCEEDED if all buffer frames are pinned
 *              - HASHTBLERROR if a hash table error occurred
 *          The page number of the newly allocated page to the caller via the pageNo parameter
 *          A pointer to the buffer frame allocated for the page via the page parameter
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status = OK;
    int frame;
    //allocate an empty page in the specified file by invoking the file->allocatePage() method
    //also returns the page number of the newly allocated page to the caller via the pageNo parameter
    status = file->allocatePage(pageNo);
    if(status != OK){
        return status;
    }
    //Then allocBuf() is called to obtain a buffer pool frame
    status = allocBuf(frame);
    if(status != OK){
        return status;
    }
    //an entry is inserted into the hash table
    status = hashTable->insert(file, pageNo, frame);
    if(status != OK){
        return status;
    }
    //and Set() is invoked on the frame to set it up properly
    bufTable[frame].Set(file, pageNo);
    //a pointer to the buffer frame allocated for the page via the page parameter
    page = &bufPool[frame];
    //return the status
    return status;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


