package com.goodworkalan.memento.serializable;

public class Address
{
    private String street;
    
    private String city;
    
    private String state;
    
    private String zip;
    
    public void setState(String state)
    {
        this.state = state;
    }
    
    public String getState()
    {
        return state;
    }
    
    public void setCity(String city)
    {
        this.city = city;
    }
    
    public String getCity()
    {
        return city;
    }
    
    public void setStreet(String street)
    {
        this.street = street;
    }
    
    public String getStreet()
    {
        return street;
    }
    
    public void setZip(String zip)
    {
        this.zip = zip;
    }
    
    public String getZip()
    {
        return zip;
    }
}
