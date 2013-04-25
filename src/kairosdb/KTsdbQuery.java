package net.opentsdb.kairosdb;


import net.opentsdb.core.DataPoint;
import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.TSDB;
import org.kairosdb.core.datastore.CachedSearchResult;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;

import com.google.common.collect.SetMultimap;

public class KTsdbQuery extends TsdbQuery
	{
	private long m_startTime;
	private long m_endTime;

	public KTsdbQuery(final TSDB tsdb, String metric, long startTime, long endTime, SetMultimap<String, String> tags)
		{
		super(tsdb);

		Map<String, String> mapTags = createMapOfTags(tags);
		setStartTime(startTime);
		setEndTime(endTime);
		setTimeSeries(metric, mapTags, null, false);

		m_startTime = startTime;
		m_endTime = endTime;
		}

	public void run(CachedSearchResult cachedSearchResult) throws IOException
		{
		TreeMap<byte[], Span> spanMap = findSpans();

		for(Span span : spanMap.values())
			{
			cachedSearchResult.startDataPointSet(span.getInclusiveTags());
			for (DataPoint dataPoint : span)
				{
				if (dataPoint.timestamp() < m_startTime || dataPoint.timestamp() > m_endTime)
					{
					// Remove data points not in the time range
					continue;
					}

				//Convert timestamps back to milliseconds
				if (dataPoint.isInteger())
					{
					cachedSearchResult.addDataPoint(dataPoint.timestamp() *1000, dataPoint.longValue());
					}
				else
					{
					cachedSearchResult.addDataPoint(dataPoint.timestamp() *1000, dataPoint.doubleValue());
					}
				}
			}

		cachedSearchResult.endDataPoints();
		}

	private Map<String, String> createMapOfTags(SetMultimap<String, String> tags)
		{
		HashMap<String, String> ret = new HashMap<String, String>();
		for (String key : tags.keySet())
			{
			boolean first = true;
			StringBuilder values = new StringBuilder();
			for (String value : tags.get(key))
				{
				if (!first)
					values.append("|");
				first = false;
				values.append(value);
				}

			ret.put(key, values.toString());
			}

		return (ret);
		}
	}
