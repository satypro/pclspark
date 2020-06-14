package com.pclspark.model;

import java.io.Serializable;
import java.math.BigInteger;

public class PointCloud implements Serializable
{
    int regionid;
    long mortoncode;
    int pointid;
    float x;
    float y;
    float z;
    private double distance;

    public long getMortoncode()
    {
        return mortoncode;
    }

    public void setMortoncode(long mortoncode)
    {
        this.mortoncode = mortoncode;
    }

    public int getPointid()
    {
        return pointid;
    }

    public void setPointid(int pointid)
    {
        this.pointid = pointid;
    }

    public float getX()
    {
        return x;
    }

    public void setX(float x)
    {
        this.x = x;
    }

    public float getY()
    {
        return y;
    }

    public void setY(float y)
    {
        this.y = y;
    }

    public float getZ()
    {
        return z;
    }

    public void setZ(float z)
    {
        this.z = z;
    }

    public int getRegionid()
    {
        return regionid;
    }

    public void setRegionid(int regionid)
    {
        this.regionid = regionid;
    }

    public double getDistance()
    {
        return distance;
    }

    public void setDistance(double distance)
    {
        this.distance = distance;
    }
}
