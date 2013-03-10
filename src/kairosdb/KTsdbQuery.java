package net.opentsdb.kairosdb;


import net.opentsdb.core.DataPoint;
import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.TSDB;
import org.kairosdb.core.datastore.CachedSearchResult;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class KTsdbQuery extends TsdbQuery
	{
	private long m_startTime;
	private long m_endTime;
	
	public KTsdbQuery(final TSDB tsdb, String metric, long startTime, long endTime, Map<String, String> tags)
		{
		super(tsdb);
		
		setStartTime(startTime);
		setEndTime(endTime);
		setTimeSeries(metric, tags, null, false);
		
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

				if (dataPoint.isInteger())
					{
					cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.longValue());
					}
				else
					{
					cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.doubleValue());
					}
				}
			}

		cachedSearchResult.endDataPoints();
		}
	}
