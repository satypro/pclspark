package com.pclspark.model;

import java.io.Serializable;

public class PointCloudNormalized implements Serializable
{
    private int regionid;
    private long mortoncode;
    private long pointid;
    private long x;
    private long y;
    private long z;

    public int getRegionid()
    {
        return regionid;
    }

    public void setRegionid(int regionIid)
    {
        this.regionid = regionIid;
    }

    public long getMortoncode()
    {
        return mortoncode;
    }

    public void setMortoncode(long mortoncode)
    {
        this.mortoncode = mortoncode;
    }

    public long getPointid()
    {
        return pointid;
    }

    public void setPointid(long pointid)
    {
        this.pointid = pointid;
    }

    public long getX()
    {
        return x;
    }

    public void setX(long x)
    {
        this.x = x;
    }

    public long getY()
    {
        return y;
    }

    public void setY(long y)
    {
        this.y = y;
    }

    public long getZ()
    {
        return z;
    }

    public void setZ(long z)
    {
        this.z = z;
    }

    public double getDistance()
    {
        return distance;
    }

    public void setDistance(double distance)
    {
        this.distance = distance;
    }

    private double distance;
}
