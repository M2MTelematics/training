package TFIDF_RT;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class Tfidf_Retake_Comparator  extends WritableComparator {

		public Tfidf_Retake_Comparator() {
			super(Tfidf.class,true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			Tfidf e1 = (Tfidf) a;
			Tfidf e2 = (Tfidf) b;
			return e1.getWord().compareTo(e2.getWord());
		}
		


	}
