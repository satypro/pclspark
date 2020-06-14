package com.pclspark.model;

import com.pclspark.morton.Morton3D;

import java.io.Serializable;

public class Coordinate implements Serializable, Comparable<Coordinate>
{
    private static final long serialVersionUID = 1234567L;
    private float x;
    private float y;
    private float z;
    private long m;
    private Morton3D morton = new Morton3D();

    public Coordinate(float x, float y, float z)
    {
        this.x = x;
        this.y = y;
        this.z = z;
        this.m = morton.encode((int)(x*10000000),(int)(y*10000000),(int)(z*10000000));
    }

    public long getM() {
        return m;
    }

    public void setM(long m) {
        this.m = m;
    }

    public float getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public float getZ() {
        return z;
    }

    public void setZ(int z) {
        this.z = z;
    }

    public String toString()
    {
        return String.valueOf(m)
                + "," + String.valueOf(x)
                + "," + String.valueOf(y)
                + "," + String.valueOf(z);
    }

    @Override
    public int compareTo(Coordinate o)
    {
        return this.getM() > o.getM() ? 1 : 0;
    }
}