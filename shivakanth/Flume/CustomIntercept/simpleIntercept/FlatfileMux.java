package CustomIntercept.simpleIntercept;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


public class FlatfileMux implements Interceptor{

	private final String _header = "location";
	private final String _defaultLocation = "IN";
	
	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	public Event intercept(Event event) {
		// TODO Auto-generated method stub
		String location = _defaultLocation;
		byte[] eventBody = event.getBody();
		String[] cols = new String(eventBody).split(",");
		location = cols[0];
				
		Map<String,String> headers= event.getHeaders();
	    headers.put(_header, location);
			
	     event.setHeaders(headers);
			
	     return event;
	}

	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
		 for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();)
	     {
	       Event next = intercept(iterator.next());
	       if (next == null)
	       {
	          iterator.remove();
	       }
	     }

	     return events;
	}
	
	public static class Builder implements Interceptor.Builder {

		public void configure(Context context) {
			// TODO Auto-generated method stub
			
		}

		public Interceptor build() {
			// TODO Auto-generated method stub
			return new FlatfileMux();
		}
		
		
	}

}
