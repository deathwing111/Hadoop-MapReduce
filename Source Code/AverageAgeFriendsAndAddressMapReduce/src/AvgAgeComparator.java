import java.util.Comparator;
import java.util.Map.Entry;

/**
 * 
 * @author Rohini Yadav
 * Comparator is used to sort the users according 
 * to the descending order of average age of their friends.
 * Same average age is sorted according to the user id.
 *
 */
public class AvgAgeComparator implements Comparator<Entry<String, Long>>{

	@Override
	public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
		// If two records have same mutual friends count then sort according to their Id 
		if (o2.getValue().compareTo(o1.getValue()) == 0)
		{
			String splitArr1[] = o1.getKey().split(",");
			String splitArr2[] = o2.getKey().split(",");
			
			Integer userId1 = Integer.parseInt((splitArr1[0].trim()));
			Integer userId2 = Integer.parseInt((splitArr2[0].trim()));
			
			return userId1.compareTo(userId2);
		}
		else
		{
			return o2.getValue().compareTo(o1.getValue());
		}
	}


}
