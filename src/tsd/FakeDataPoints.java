package net.opentsdb.tsd;

import com.google.common.collect.HashMultimap;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FakeDataPoints extends ArrayList<DataPoint>
	implements DataPoints
{
	private String metricName;
	private HashMultimap<String, String> tags;

	public FakeDataPoints(String metricName, HashMultimap<String, String> tags)
	{
		this.metricName = metricName;
		this.tags = tags;
	}

	@Override
	public String metricName()
	{
		return metricName;
	}

	@Override
	public Map<String, String> getTags()
	{
		return null;
	}

	@Override
	public List<String> getAggregatedTags()
	{
		return null;
	}

	@Override
	public HashMultimap<String, String> getInclusiveTags()
	{
		return tags;
	}

	@Override
	public int aggregatedSize()
	{
		return 0;
	}

	@Override
	public long timestamp(int i)
	{
		return 0;
	}

	@Override
	public boolean isInteger(int i)
	{
		return false;
	}

	@Override
	public long longValue(int i)
	{
		return 0;
	}

	@Override
	public double doubleValue(int i)
	{
		return 0;
	}
}
