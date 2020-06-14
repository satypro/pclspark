package com.pclspark.model;

import com.pclspark.model.PointCloud;

import java.io.Serializable;
import java.util.List;

public class PointCloudFound implements Serializable
{
    private int pointid;
    private List<PointCloud> pointClouds;

    public PointCloudFound(int pointid, List<PointCloud> pointClouds)
    {
        this.pointid = pointid;
        this.pointClouds = pointClouds;
    }

    public int getPointid()
    {
        return pointid;
    }

    public void setPointid(int pointid)
    {
        this.pointid = pointid;
    }

    public List<PointCloud> getPointClouds()
    {
        return pointClouds;
    }

    public void setPointClouds(List<PointCloud> pointClouds)
    {
        this.pointClouds = pointClouds;
    }
}
