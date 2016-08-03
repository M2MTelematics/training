package TFIDF_RT;


import org.apache.hadoop.mapreduce.Partitioner;

public class Tfidf_Partitioner extends Partitioner<Tfidf, Tfidf>{

	@Override
	public int getPartition(Tfidf key, Tfidf value, int numPartitions) {
		// TODO Auto-generated method stub
		return key.getWord().hashCode() & Integer.MAX_VALUE % numPartitions;
	}

}
