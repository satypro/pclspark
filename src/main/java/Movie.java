import java.io.Serializable;

public class Movie implements Serializable
{
    private String name;
    private Double rating;
    private String timestamp;

    public Movie(String name, Double rating, String timestamp)
    {
        super();
        this.name = name;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public Movie()
    {
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}