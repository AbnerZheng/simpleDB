package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final RandomAccessFile fileInputStream;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td){
        RandomAccessFile fileInputStream1;
        this.file = f;
        this.tupleDesc = td;
        try {
        	fileInputStream1 = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
        	fileInputStream1 = null;
            e.printStackTrace();
        }
        this.fileInputStream = fileInputStream1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if(numPages() <= pid.getPageNumber() || pid.getPageNumber() < 0){
            throw new IllegalArgumentException();
        }

        final byte[] data = HeapPage.createEmptyPageData();
        try {
        	fileInputStream.seek(BufferPool.getPageSize() * pid.getPageNumber());
            fileInputStream.read(data,0 , BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        final int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        this.fileInputStream.seek(offset);
        this.fileInputStream.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int pageNum = 0;
        while(pageNum < this.numPages()) {
            final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), pageNum), Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                return new ArrayList<Page>(){{add(page);}};
            }
            pageNum ++;
        }
        final int pid = this.numPages();
        final byte[] emptyPageData = HeapPage.createEmptyPageData();
        final HeapPage heapPage = new HeapPage(new HeapPageId(getId(), pid), emptyPageData);
        heapPage.insertTuple(t);
        writePage(heapPage);

        return new ArrayList<Page>(){{add(heapPage);}};
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
                                                                              TransactionAbortedException {
        final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<Page>(){{add(page);}};
    }
    public class HeapFileIterator extends AbstractDbFileIterator {
        private int nextPageNumber;
        private TransactionId transactionId;
        private Iterator<Tuple> pageIter;

        public HeapFileIterator(TransactionId tid){
            transactionId = tid;
            this.nextPageNumber = 0;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
        	if(this.pageIter == null){
        		return null;
            }
            if(pageIter.hasNext()){
                return pageIter.next();
            }
            while(++this.nextPageNumber < numPages()) {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), nextPageNumber), Permissions.READ_ONLY);
                this.pageIter = page.iterator();
                if(pageIter.hasNext()){
                    return pageIter.next();
                }
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
        	HeapPage page =(HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), nextPageNumber), Permissions.READ_ONLY);

            this.pageIter = page.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
        	this.nextPageNumber = 0;
        	open();
        }

        @Override
        public void close() {
           super.close();
           this.pageIter = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(tid);
    }

}

