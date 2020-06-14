package com.pclspark.model;

import java.io.Serializable;

public class PointFeature implements Serializable
{
    private long regionid;
    private long pointid;
    private double cl;
    private double cp;
    private double cs;
    private double omnivariance;
    private double anisotropy;
    private double eigenentropy;
    private double changeofcurvature;
    private int label;

    public PointFeature(
            long regionId,
            long pointId,
            double cl,
            double cp,
            double cs,
            double omnivariance,
            double anisotropy,
            double eigenentropy,
            double changeOfCurvature,
            int label)
    {
        this.setRegionid(regionId);
        this.setPointid(pointId);
        this.setCp(cp);
        this.setCs(cs);
        this.setCl(cl);
        this.setOmnivariance(omnivariance);
        this.setAnisotropy(anisotropy);
        this.setEigenentropy(eigenentropy);
        this.setChangeofcurvature(changeOfCurvature);
        this.label = label;
    }

    public double getCl()
    {
        return cl;
    }

    public void setCl(double cl)
    {
        this.cl = cl;
    }

    public double getCp()
    {
        return cp;
    }

    public void setCp(double cp)
    {
        this.cp = cp;
    }

    public double getCs()
    {
        return cs;
    }

    public void setCs(double cs)
    {
        this.cs = cs;
    }

    public double getOmnivariance()
    {
        return omnivariance;
    }

    public void setOmnivariance(double omnivariance)
    {
        this.omnivariance = omnivariance;
    }

    public double getAnisotropy()
    {
        return anisotropy;
    }

    public void setAnisotropy(double anisotropy)
    {
        this.anisotropy = anisotropy;
    }

    public double getEigenentropy()
    {
        return eigenentropy;
    }

    public void setEigenentropy(double eigenentropy)
    {
        this.eigenentropy = eigenentropy;
    }

    public int getLabel()
    {
        return label;
    }

    public void setLabel(int label)
    {
        this.label = label;
    }

    public long getRegionid()
    {
        return regionid;
    }

    public void setRegionid(long regionid)
    {
        this.regionid = regionid;
    }

    public long getPointid()
    {
        return pointid;
    }

    public void setPointid(long pointid)
    {
        this.pointid = pointid;
    }

    public double getChangeofcurvature()
    {
        return changeofcurvature;
    }

    public void setChangeofcurvature(double changeofcurvature)
    {
        this.changeofcurvature = changeofcurvature;
    }
}