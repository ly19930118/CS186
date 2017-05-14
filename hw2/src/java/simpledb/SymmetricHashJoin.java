package simpledb;

import java.util.*;

/**
 * The SymmetricHashJoin operator implements the symmetric hash join operation.
 */
public class SymmetricHashJoin extends Operator {
    private JoinPredicate pred;
    private DbIterator child1, child2;
    private TupleDesc comboTD;

    private HashMap<Object, ArrayList<Tuple>> leftMap = new HashMap<Object, ArrayList<Tuple>>();
    private HashMap<Object, ArrayList<Tuple>> rightMap = new HashMap<Object, ArrayList<Tuple>>();

    private DbIterator inner, outer;
    private Tuple in;
    //private Iterator<Tuple> out_lst;
    private int index;

    private boolean first;
    private int inner_field, outer_field;
    private int field1,field2;

     /**
     * Constructor. Accepts children to join and the predicate to join them on.
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public SymmetricHashJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;
        comboTD = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());

        this.inner = null;
        in = null;
        index = 0;



        this.inner_field = p.getField1();
        this.outer_field = p.getField2();
        this.field1 = p.getField1();
        this.field2 = p.getField2();

        first = true;

        this.inner = child1;
        this.outer = child2;
    }

    public TupleDesc getTupleDesc() {
        return comboTD;
    }

    /**
     * Opens the iterator.
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // IMPLEMENT ME
        child1.open();
        child2.open();
        super.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        // IMPLEMENT ME
        super.close();
        child2.close();
        child1.close();
        
    }

    /**
     * Rewinds the iterator. You should not be calling this method for this join. 
     */
    public void rewind() throws DbException, TransactionAbortedException {
        //System.out.println("-------------------rewind");

        child1.rewind();
        child2.rewind();
        this.leftMap.clear();
        this.rightMap.clear();

        inner = child1;
        outer = child2;
        index = 0;
        in = null;       
        first = true; 
        this.inner_field = this.field1;
        this.outer_field = this.field2
        ;
    }

    /**
     * Fetches the next tuple generated by the join, or null if there are no 
     * more tuples.  Logically, this is the next tuple in r1 cross r2 that
     * satifies the join predicate.
     *
     * Note that the tuples returned from this particular implementation are
     * simply the concatenation of joining tuples from the left and right
     * relation.  Therefore, there will be two copies of the join attribute in
     * the results.
     *
     * For example, joining {1,2,3} on equality of the first column with {1,5,6}
     * will return {1,2,3,1,5,6}.
     */

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // System.out.println(" ");
        // System.out.println("new");
       
       do{

            //System.out.println("curr " + in);
            
            if(in != null){
            
                
                int key = in.getField(inner_field).hashCode();

                // if(leftMap.get(key) == null)
                //     leftMap.put(key, new ArrayList<Tuple>());
                // leftMap.get(key).add(in);

                ArrayList<Tuple> out_lst = rightMap.get(key);
     
                if(out_lst!=null && out_lst.size()!=0 && index<out_lst.size()){

                    //System.out.println(index);

                    for(int j=index;j<out_lst.size();j++){
                        
                        Tuple out = out_lst.get(j);

                        //System.out.println(in + " " + out + " size =" + out_lst.size());

                        if(pred.filter(in,out)){
                            int td1n = in.getTupleDesc().numFields();
                            int td2n = out.getTupleDesc().numFields();
                            Tuple t1 = in;
                            Tuple t2 = out;

                            if(!first){
                                t1 = out;
                                t2 = in;
                                td1n = out.getTupleDesc().numFields();
                                td2n = in.getTupleDesc().numFields();
                            }

                            // set fields in combined tuple
                            Tuple t = new Tuple(comboTD);
                            for (int i = 0; i < td1n; i++)
                                t.setField(i, t1.getField(i));
                            for (int i = 0; i < td2n; i++)
                                t.setField(td1n + i, t2
                                    .getField(i));

                            //System.out.println("results = " + t);
                            
                            index = j+1;              

                            //System.out.println("index : " + index);
                            //System.out.println("child 1 :" + child1.next() + " child 2: " + child2.next());
                            return t;
                        }
                    }
                }
            }
            
            //System.out.println("switched index 0");            
            
            switchRelations();

            streamTuple();
            
            index = 0;

            

        }while(inner.hasNext() || outer.hasNext() || in != null);

        return null;
    }

    // public Tuple curr(){
    //     return this.inner;
    // }
    public void streamTuple() throws TransactionAbortedException, DbException{
        if(inner.hasNext()){
            this.in = inner.next();

            int key = in.getField(inner_field).hashCode();

            if(leftMap.get(key) == null)
                 leftMap.put(key, new ArrayList<Tuple>());
             leftMap.get(key).add(in);

        }else
            this.in = null;
    }

    private void switchRelations() throws TransactionAbortedException, DbException {
        // IMPLEMENT ME
        HashMap<Object, ArrayList<Tuple>> temp = rightMap;
        rightMap = leftMap;
        leftMap = temp;

        int temp_field = outer_field;
        outer_field = inner_field;
        inner_field = temp_field;

        DbIterator temp_outer = outer;
        outer = inner;
        inner = temp_outer;

        first = !first;

    }

    // protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    //     // IMPLEMENT ME

    //     while(child1.hasNext() || child2.hasNext()) {
    //         if(child1.hasNext() == false){
    //             switchRelations();
    //             continue;
    //         }
    //         Tuple in = child1.next();
    //         int key = in.getField(inner_field).hashCode();

    //         if(leftMap.get(key) == null)
    //             leftMap.put(key, new ArrayList<Tuple>());
    //         leftMap.get(key).add(in);

    //         //System.out.println(in);

    //         ArrayList<Tuple> out_lst = rightMap.get(key);

    //         if(out_lst != null && out_lst.size()!=0){
    //             for(Tuple out : out_lst){
    //                 if(pred.filter(in,out)){

    //                     int td1n = in.getTupleDesc().numFields();
    //                     int td2n = out.getTupleDesc().numFields();

    //                     Tuple t1 = in;
    //                     Tuple t2 = out;

    //                     if(!first){
    //                         t1 = out;
    //                         t2 = in;
    //                         td1n = out.getTupleDesc().numFields();
    //                         td2n = in.getTupleDesc().numFields();
    //                     }

    //                 // set fields in combined tuple
    //                     Tuple t = new Tuple(comboTD);
    //                     for (int i = 0; i < td1n; i++){
    //                         t.setField(i, t1.getField(i));
    //                     }
    //                     for (int i = 0; i < td2n; i++){
    //                         t.setField(td1n + i, t2.getField(i));
    //                     }
    //                     //switchRelations();
    //                     return t;
    //                 }
    //             }
    //         }

    //         switchRelations();

    //     }

    //     return null;
    // }

    /**
     * Switches the inner and outer relation.
     */
    

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}