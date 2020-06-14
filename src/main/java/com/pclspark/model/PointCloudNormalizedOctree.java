package com.pclspark.model;

import java.io.Serializable;

public class PointCloudNormalizedOctree implements Serializable
{
    private String regionid;
    private long mortoncode;
    private long pointid;
    private long x;
    private long y;
    private long z;
    private float xo;
    private float yo;
    private float zo;

    public String getRegionid()
    {
        return regionid;
    }

    public void setRegionid(String regionIid)
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

    public float getXo() {
        return xo;
    }

    public void setXo(float xo) {
        this.xo = xo;
    }

    public float getYo() {
        return yo;
    }

    public void setYo(float yo) {
        this.yo = yo;
    }

    public float getZo() {
        return zo;
    }

    public void setZo(float zo) {
        this.zo = zo;
    }
}
