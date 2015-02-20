package io.mesosphere.mesos.frameworks.cassandra.files;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

public class CachedFileResourceTest {
    @Test
    public void testCachedFileResources() {
        Response response = new JreFileResourceController().jdkTar("macosx");
        Assert.assertEquals(response.getStatus(), 200);

        Assert.assertEquals(CachedFileResource.getStatsDownloads(), 1);
        long ts = CachedFileResource.getStatsLastDownload();
        Assert.assertTrue(ts != 0L);
        long sz = CachedFileResource.getStatsDownloadedBytes();
        Assert.assertTrue(sz != 0L);

        response = new JreFileResourceController().jdkTar("macosx");
        Assert.assertEquals(response.getStatus(), 200);

        Assert.assertEquals(CachedFileResource.getStatsDownloads(), 1);

        //

        response = new CassandraFileResourceController().cassandraTar("2.1.2");
        Assert.assertEquals(response.getStatus(), 200);

        Assert.assertEquals(CachedFileResource.getStatsDownloads(), 2);
        Assert.assertTrue(ts < CachedFileResource.getStatsLastDownload());
        Assert.assertTrue(sz < CachedFileResource.getStatsDownloadedBytes());

        response = new CassandraFileResourceController().cassandraTar("2.1.2");
        Assert.assertEquals(response.getStatus(), 200);

        Assert.assertEquals(CachedFileResource.getStatsDownloads(), 2);
    }
}