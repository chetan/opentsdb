package net.opentsdb.kairosdb;


import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.stumbleupon.async.Callback;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.exception.DatastoreException;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.kairosdb.core.datastore.CachedSearchResult;
import org.kairosdb.core.datastore.DataPointRow;

import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;

import java.util.List;

public class HBaseDatastore implements Datastore
{
	public static final String TIMESERIES_TABLE_PROPERTY = "kairosdb.datastore.hbase.timeseries_table";
	public static final String UNIQUEIDS_TABLE_PROPERTY = "kairosdb.datastore.hbase.uinqueids_table";
	public static final String ZOO_KEEPER_QUORUM_PROPERTY = "kairosdb.datastore.hbase.zoo_keeper_quorum";
	public static final String ZOO_KEEPER_BASE_PROPERTY = "kairosdb.datastore.hbase.zoo_keeper_base_dir";
	public static final String AUTO_CREATE_METRIC_PROPERTY = "kairosdb.datastore.hbase.auto_create_metrics";

	private TSDB m_tsdb;

	@Inject
	public HBaseDatastore(@Named(TIMESERIES_TABLE_PROPERTY) String timeSeriesTable,
	                      @Named(UNIQUEIDS_TABLE_PROPERTY) String uidTable,
	                      @Named(ZOO_KEEPER_QUORUM_PROPERTY) String zkQuorum,
	                      @Named(ZOO_KEEPER_BASE_PROPERTY) String zkBase,
	                      @Named(AUTO_CREATE_METRIC_PROPERTY) boolean autoCreateMetrics) throws DatastoreException
		{
		HBaseClient hbaseClient;

		if ((zkBase != null) && (!zkBase.equals("")))
			hbaseClient = new HBaseClient(zkQuorum, zkBase);
		else
			hbaseClient = new HBaseClient(zkQuorum);

		//hbaseClient.setFlushInterval((short)0);

		if (autoCreateMetrics)
			System.setProperty("tsd.core.auto_create_metrics", "true");

		m_tsdb = new TSDB(hbaseClient, timeSeriesTable, uidTable);
		}


	@Override
	public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
		final class PutErrback implements Callback<Exception, Exception>
			{
			public DatastoreException call(final Exception arg)
				{
				return new DatastoreException(arg);
				}

			public String toString()
				{
				return "report error";
				}
			}

		for (DataPoint dp : dps.getDataPoints())
			{
			//Need to convert the timestamp to seconds
			long timestamp = dp.getTimestamp() / 1000;

			if (dp.isInteger())
				{
				m_tsdb.addPoint(dps.getName(), timestamp, dp.getLongValue(),
						dps.getTags()).addErrback(new PutErrback());
				}
			else
				{
				m_tsdb.addPoint(dps.getName(), timestamp, (float) dp.getDoubleValue(),
						dps.getTags()).addErrback(new PutErrback());
				}
			}
		}

	@Override
	public Iterable<String> getMetricNames() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestMetrics("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public Iterable<String> getTagNames() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestTagNames("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public Iterable<String> getTagValues() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestTagValues("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public List<DataPointRow> queryDatabase(DatastoreMetricQuery query, CachedSearchResult cachedSearchResult) throws DatastoreException
		{
		try
			{
			KTsdbQuery tsdbquery = new KTsdbQuery(m_tsdb, query.getName(), query.getStartTime() / 1000, query.getEndTime() / 1000, query.getTags());

			tsdbquery.run(cachedSearchResult);
			return cachedSearchResult.getRows();
			}
		catch (Exception e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public void close() throws DatastoreException
		{
		try
			{
			m_tsdb.shutdown().join();
			}
		catch (Exception e)
			{
			throw new DatastoreException(e);
			}
		}
	}
