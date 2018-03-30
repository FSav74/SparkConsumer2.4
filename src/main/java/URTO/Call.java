//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.5-2 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.05.16 at 07:42:17 PM CEST 
//


package URTO;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="uidSupplier" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="sourcePlatformCode" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="targetPlatformCode" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="hardwareTimestamp" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="platfomRelayTimestamp" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="caller" type="{http://not.ima.tm.fr/telematic}Caller" minOccurs="0"/>
 *         &lt;element name="contractualContext" type="{http://not.ima.tm.fr/telematic}ContractualContext"/>
 *         &lt;element name="request" type="{http://not.ima.tm.fr/telematic}Request"/>
 *         &lt;element name="location" type="{http://not.ima.tm.fr/telematic}LocationHeader" minOccurs="0"/>
 *         &lt;element name="systemInformation" type="{http://not.ima.tm.fr/telematic}SystemInformation" minOccurs="0"/>
 *         &lt;element name="initialContact" type="{http://not.ima.tm.fr/telematic}Contact"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "uidSupplier",
    "sourcePlatformCode",
    "targetPlatformCode",
    "hardwareTimestamp",
    "platfomRelayTimestamp",
    "caller",
    "contractualContext",
    "request",
    "location",
    "systemInformation",
    "initialContact"
})
@XmlRootElement(name = "call")
public class Call {

    @XmlElement(required = true)
    protected String uidSupplier;
    @XmlElement(required = true)
    protected String sourcePlatformCode;
    @XmlElement(required = true)
    protected String targetPlatformCode;
    @XmlElement(required = true)
    protected String hardwareTimestamp;
    protected String platfomRelayTimestamp;
    protected Caller caller;
    @XmlElement(required = true)
    protected ContractualContext contractualContext;
    @XmlElement(required = true)
    protected Request request;
    protected LocationHeader location;
    protected SystemInformation systemInformation;
    @XmlElement(required = true)
    protected Contact initialContact;

    /**
     * Gets the value of the uidSupplier property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUidSupplier() {
        return uidSupplier;
    }

    /**
     * Sets the value of the uidSupplier property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUidSupplier(String value) {
        this.uidSupplier = value;
    }

    /**
     * Gets the value of the sourcePlatformCode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getSourcePlatformCode() {
        return sourcePlatformCode;
    }

    /**
     * Sets the value of the sourcePlatformCode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setSourcePlatformCode(String value) {
        this.sourcePlatformCode = value;
    }

    /**
     * Gets the value of the targetPlatformCode property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTargetPlatformCode() {
        return targetPlatformCode;
    }

    /**
     * Sets the value of the targetPlatformCode property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTargetPlatformCode(String value) {
        this.targetPlatformCode = value;
    }

    /**
     * Gets the value of the hardwareTimestamp property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getHardwareTimestamp() {
        return hardwareTimestamp;
    }

    /**
     * Sets the value of the hardwareTimestamp property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setHardwareTimestamp(String value) {
        this.hardwareTimestamp = value;
    }

    /**
     * Gets the value of the platfomRelayTimestamp property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPlatfomRelayTimestamp() {
        return platfomRelayTimestamp;
    }

    /**
     * Sets the value of the platfomRelayTimestamp property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPlatfomRelayTimestamp(String value) {
        this.platfomRelayTimestamp = value;
    }

    /**
     * Gets the value of the caller property.
     * 
     * @return
     *     possible object is
     *     {@link Caller }
     *     
     */
    public Caller getCaller() {
        return caller;
    }

    /**
     * Sets the value of the caller property.
     * 
     * @param value
     *     allowed object is
     *     {@link Caller }
     *     
     */
    public void setCaller(Caller value) {
        this.caller = value;
    }

    /**
     * Gets the value of the contractualContext property.
     * 
     * @return
     *     possible object is
     *     {@link ContractualContext }
     *     
     */
    public ContractualContext getContractualContext() {
        return contractualContext;
    }

    /**
     * Sets the value of the contractualContext property.
     * 
     * @param value
     *     allowed object is
     *     {@link ContractualContext }
     *     
     */
    public void setContractualContext(ContractualContext value) {
        this.contractualContext = value;
    }

    /**
     * Gets the value of the request property.
     * 
     * @return
     *     possible object is
     *     {@link Request }
     *     
     */
    public Request getRequest() {
        return request;
    }

    /**
     * Sets the value of the request property.
     * 
     * @param value
     *     allowed object is
     *     {@link Request }
     *     
     */
    public void setRequest(Request value) {
        this.request = value;
    }

    /**
     * Gets the value of the location property.
     * 
     * @return
     *     possible object is
     *     {@link LocationHeader }
     *     
     */
    public LocationHeader getLocation() {
        return location;
    }

    /**
     * Sets the value of the location property.
     * 
     * @param value
     *     allowed object is
     *     {@link LocationHeader }
     *     
     */
    public void setLocation(LocationHeader value) {
        this.location = value;
    }

    /**
     * Gets the value of the systemInformation property.
     * 
     * @return
     *     possible object is
     *     {@link SystemInformation }
     *     
     */
    public SystemInformation getSystemInformation() {
        return systemInformation;
    }

    /**
     * Sets the value of the systemInformation property.
     * 
     * @param value
     *     allowed object is
     *     {@link SystemInformation }
     *     
     */
    public void setSystemInformation(SystemInformation value) {
        this.systemInformation = value;
    }

    /**
     * Gets the value of the initialContact property.
     * 
     * @return
     *     possible object is
     *     {@link Contact }
     *     
     */
    public Contact getInitialContact() {
        return initialContact;
    }

    /**
     * Sets the value of the initialContact property.
     * 
     * @param value
     *     allowed object is
     *     {@link Contact }
     *     
     */
    public void setInitialContact(Contact value) {
        this.initialContact = value;
    }

}
