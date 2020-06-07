import java.io.Serializable;

public class PointCloudRegions implements Serializable
{
    private long regionid;
    private long pointid;
    private long morton;
    private int label;
    private long x;
    private long y;
    private long z;
    private float xo;
    private float yo;
    private float zo;
    
    public long getX() {
        return x;
    }

    public void setX(long x) {
        this.x = x;
    }

    public long getY() {
        return y;
    }

    public void setY(long y) {
        this.y = y;
    }

    public long getZ() {
        return z;
    }

    public void setZ(long z) {
        this.z = z;
    }

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

    public long getRegionid() {
        return regionid;
    }

    public void setRegionid(long regionid) {
        this.regionid = regionid;
    }

    public long getPointid() {
        return pointid;
    }

    public void setPointid(long pointid) {
        this.pointid = pointid;
    }

    public long getMorton() {
        return morton;
    }

    public void setMorton(long morton) {
        this.morton = morton;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }
}
