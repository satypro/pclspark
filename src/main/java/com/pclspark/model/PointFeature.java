package com.pclspark.model;

import java.io.Serializable;

public class PointFeature implements Serializable
{
    private float cl;
    private float cp;
    private float cs;
    private float omnivariance;
    private float anisotropy;
    private float eigenentropy;
    private float changeofcurvature;
    private int label;
    private double [] features;

    public PointFeature(
            float cl,
            float cp,
            float cs,
            float omnivariance,
            float anisotropy,
            float eigenentropy,
            float changeOfCurvature,
            int label)
    {
        this.setCp(cp);
        this.setCs(cs);
        this.setCl(cl);
        this.setOmnivariance(omnivariance);
        this.setAnisotropy(anisotropy);
        this.setEigenentropy(eigenentropy);
        this.setChangeofcurvature(changeOfCurvature);
        this.setLabel(label);
        this.features = new double[7];
        features[0] = cl;
        features[1] = cp;
        features[2] = cs;
        features[3] = omnivariance;
        features[4] = anisotropy;
        features[5] = eigenentropy;
        features[6] = changeOfCurvature;
    }

    public float getCl()
    {
        return cl;
    }

    public void setCl(float cl)
    {
        this.cl = cl;
    }

    public float getCp()
    {
        return cp;
    }

    public void setCp(float cp)
    {
        this.cp = cp;
    }

    public float getCs()
    {
        return cs;
    }

    public void setCs(float cs)
    {
        this.cs = cs;
    }

    public float getOmnivariance()
    {
        return omnivariance;
    }

    public void setOmnivariance(float omnivariance)
    {
        this.omnivariance = omnivariance;
    }

    public float getAnisotropy()
    {
        return anisotropy;
    }

    public void setAnisotropy(float anisotropy)
    {
        this.anisotropy = anisotropy;
    }

    public float getEigenentropy()
    {
        return eigenentropy;
    }

    public void setEigenentropy(float eigenentropy)
    {
        this.eigenentropy = eigenentropy;
    }

    public float getChangeofcurvature()
    {
        return changeofcurvature;
    }

    public void setChangeofcurvature(float changeofcurvature)
    {
        this.changeofcurvature = changeofcurvature;
    }

    public int getLabel()
    {
        return label;
    }

    public void setLabel(int label)
    {
        this.label = label;
    }

    public double[] getFeatures()
    {
        return features;
    }

    public void setFeatures(double[] features)
    {
        this.features = features;
    }
}