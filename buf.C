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
    Status status;
    int count = 0;
    //TODO Case: if all buffer frames are pinned, maybe loop through the clock twice, 
    //           return BUFFEREXCEEDED if out of loop. Or use a bool
    while (true){
        count += 1;
        //return BUFFEREXCEEDED if all frams are pinned
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
    return status;
}

const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    // Check whether the page is already in the buffer pool
    Status status = OK;
    int frameNo = 0;
    bool isInBufferPool = hashTable->lookup(file, PageNo, frameNo);
    // Case 1, Page is not in the buffer pool



}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







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


