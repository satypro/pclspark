package com.pclspark.model;

public class PointCloudPartition
{
    private long regionid;
    private long pointid;
    private float xn;
    private float yn;
    private float zn;
    private float x;
    private float y;
    private float z;

    public float getXn() {
        return xn;
    }

    public void setXn(float xn) {
        this.xn = xn;
    }

    public float getYn() {
        return yn;
    }

    public void setYn(float yn) {
        this.yn = yn;
    }

    public float getZn() {
        return zn;
    }

    public void setZn(float zn) {
        this.zn = zn;
    }

    public float getX() {
        return x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(float y) {
        this.y = y;
    }

    public float getZ() {
        return z;
    }

    public void setZ(float z) {
        this.z = z;
    }

    public long getPointid() {
        return pointid;
    }

    public void setPointid(long pointid) {
        this.pointid = pointid;
    }

    public long getRegionid() {
        return regionid;
    }

    public void setRegionid(long regionid) {
        this.regionid = regionid;
    }
}