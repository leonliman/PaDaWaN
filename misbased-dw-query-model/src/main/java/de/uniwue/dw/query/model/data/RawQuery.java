package de.uniwue.dw.query.model.data;

import java.sql.Timestamp;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement
public class RawQuery {

  @XmlAttribute
  private String xml;

  @XmlAttribute
  private String name;

  @XmlTransient
  private Timestamp creationTime;

  @XmlTransient
  private Timestamp modifyTime;

  @XmlAttribute
  private int id;

  // for the xml serializer
  @SuppressWarnings("unused")
  private RawQuery() {
  }

  public RawQuery(int anId, String aName, String aXml, Timestamp aCreationTime,
          Timestamp aModifyTime) {
    id = anId;
    name = aName;
    xml = aXml;
    creationTime = aCreationTime;
    modifyTime = aModifyTime;
  }

  @XmlTransient
  public String getXml() {
    return xml;
  }

  public void setXml(String xml) {
    this.xml = xml;
  }

  @XmlTransient
  public String getName() {
    return name.toLowerCase();
  }

  public void setName(String name) {
    this.name = name.toLowerCase();
  }

  @XmlTransient
  public int getId() {
    return id;
  }

  @XmlTransient
  public Timestamp getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(Timestamp creationTime) {
    this.creationTime = creationTime;
  }

  @XmlTransient
  public Timestamp getModifyTime() {
    return modifyTime;
  }

  public void setModifyTime(Timestamp modifyTime) {
    this.modifyTime = modifyTime;
  }

}
