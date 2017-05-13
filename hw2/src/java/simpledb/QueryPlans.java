package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		//TupleDesc = t2.getTupleDesc();
		JoinPredicate p = new JoinPredicate(0,Predicate.Op.EQUALS,0);
		Join j = new Join(p,t1,t2);
		return j;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		Field operand = new IntField(1);
		Predicate p_filter = new Predicate(0,Predicate.Op.GREATER_THAN, operand);
		Filter f = new Filter(p_filter, t1);

		JoinPredicate p = new JoinPredicate(1,Predicate.Op.EQUALS,1);
		Join j = new Join(p, f, t2);
		return j;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME
		Field operand = new IntField(1);

		Predicate p_filter = new Predicate(2, Predicate.Op.GREATER_THAN, operand);
		
		Filter f = new Filter(p_filter, t1);

		Aggregate agg = new Aggregate(f, 1, 0, Aggregator.Op.MAX);
		return agg;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME
		Aggregate agg1 = new Aggregate(t3, 0, -1, Aggregator.Op.COUNT);
		Predicate p1 = new Predicate(0, Predicate.Op.LESS_THAN, agg1.fetchNext().getField(0));
		Filter f1 = new Filter(p1, t1);

		t3.rewind();

		Aggregate agg2 = new Aggregate(t3, 0, -1, Aggregator.Op.AVG);
		Predicate p2 = new Predicate(0, Predicate.Op.EQUALS, agg2.fetchNext().getField(0));
		Filter f2 = new Filter(p2,t2);

		JoinPredicate p = new JoinPredicate(1,Predicate.Op.GREATER_THAN_OR_EQ,1);
		Join j = new Join(p,f1,f2);

		OrderBy o = new OrderBy(0, false, j);
		return o;
	}


}