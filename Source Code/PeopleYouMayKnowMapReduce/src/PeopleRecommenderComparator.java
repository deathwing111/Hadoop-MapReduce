import java.util.Comparator;
import java.util.Map.Entry;


public class PeopleRecommenderComparator implements Comparator<Entry<String, Integer>>
{

	@Override
	public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) 
	{	
		// If two records have same mutual friends count then sort according to their Id 
		if (o2.getValue().compareTo(o1.getValue()) == 0)
		{
			Integer o1Key = Integer.parseInt(o1.getKey());
			Integer o2Key = Integer.parseInt(o2.getKey());
			
			return o1Key.compareTo(o2Key);
		}
		else
		{
			return o2.getValue().compareTo(o1.getValue());
		}
	}
}
