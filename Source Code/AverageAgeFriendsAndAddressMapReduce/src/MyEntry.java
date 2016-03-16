import java.util.Map;


public class MyEntry implements Map.Entry <String, Long>{

	private final String key;
	private Long value;
	
	public MyEntry(String key, Long value) 
	{
		this.key = key;
		this.value = value;
	}
	@Override
	public String getKey() 
	{
		return key;
	}
	@Override
	public Long getValue() 
	{
		return value;
	}
	@Override
	public Long setValue(Long value) 
	{
		Long oldVal = this.value;
		this.value = value;
		return oldVal;		
	}
}
