package simpledb;

/**
 * Chunk contains tuples that have been read in and stored to
 * minimize the number of page accesses to read/write tuples
 * for ChunkNestedLoopJoin.
 */
public class Chunk {
    private int chunkSize;
    private Tuple[] tupleArray;

    private int tnums;

    /**
     * Create a new Chunk with the specified chunkSize (int).
     * 
     * @param tupleArray
     *            the tuples that are read in and stored in 
     *            this Chunk.           
     */
    public Chunk(int chunkSize) {
        // IMPLEMENT ME
        this.chunkSize = chunkSize;
        this.tupleArray = new Tuple[chunkSize];
        this.tnums = 0;
    }

    /**
     * Load the chunk with tuples. Max number of tuples = chunkSize.
     *
     * @param The iterator that stores a table's tuples.
     */
    public void loadChunk(DbIterator iterator) throws DbException, TransactionAbortedException {
        // IMPLEMENT ME
        tnums=0;
        while(iterator.hasNext() && tnums<this.chunkSize){
            Tuple t = iterator.next();
            tupleArray[tnums++] = t;
        }
    }

    /**
     * @return The tupleArray of this Chunk.
     */
    public Tuple[] getChunkTuples() {
        // IMPLEMENT ME
        return this.tupleArray;
    }

    public int getTupleNums(){
        return this.tnums;
    }

}
