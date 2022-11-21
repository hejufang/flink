/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.flink.connectors.loghouse.service;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-03-10")
public class GetListRequest implements org.apache.thrift.TBase<GetListRequest, GetListRequest._Fields>, java.io.Serializable, Cloneable, Comparable<GetListRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetListRequest");

  private static final org.apache.thrift.protocol.TField NS_FIELD_DESC = new org.apache.thrift.protocol.TField("ns", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField READ_VALUE_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("readValueList", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField BASE_FIELD_DESC = new org.apache.thrift.protocol.TField("Base", org.apache.thrift.protocol.TType.STRUCT, (short)255);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new GetListRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new GetListRequestTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String ns; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<ReadListValue> readValueList; // required
  public @org.apache.thrift.annotation.Nullable org.apache.flink.connectors.loghouse.service.Base Base; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NS((short)1, "ns"),
    READ_VALUE_LIST((short)2, "readValueList"),
    BASE((short)255, "Base");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // NS
          return NS;
        case 2: // READ_VALUE_LIST
          return READ_VALUE_LIST;
        case 255: // BASE
          return BASE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NS, new org.apache.thrift.meta_data.FieldMetaData("ns", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.READ_VALUE_LIST, new org.apache.thrift.meta_data.FieldMetaData("readValueList", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ReadListValue.class))));
    tmpMap.put(_Fields.BASE, new org.apache.thrift.meta_data.FieldMetaData("Base", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, org.apache.flink.connectors.loghouse.service.Base.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetListRequest.class, metaDataMap);
  }

  public GetListRequest() {
  }

  public GetListRequest(
    java.lang.String ns,
    java.util.List<ReadListValue> readValueList,
    org.apache.flink.connectors.loghouse.service.Base Base)
  {
    this();
    this.ns = ns;
    this.readValueList = readValueList;
    this.Base = Base;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetListRequest(GetListRequest other) {
    if (other.isSetNs()) {
      this.ns = other.ns;
    }
    if (other.isSetReadValueList()) {
      java.util.List<ReadListValue> __this__readValueList = new java.util.ArrayList<ReadListValue>(other.readValueList.size());
      for (ReadListValue other_element : other.readValueList) {
        __this__readValueList.add(new ReadListValue(other_element));
      }
      this.readValueList = __this__readValueList;
    }
    if (other.isSetBase()) {
      this.Base = new org.apache.flink.connectors.loghouse.service.Base(other.Base);
    }
  }

  public GetListRequest deepCopy() {
    return new GetListRequest(this);
  }

  @Override
  public void clear() {
    this.ns = null;
    this.readValueList = null;
    this.Base = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getNs() {
    return this.ns;
  }

  public GetListRequest setNs(@org.apache.thrift.annotation.Nullable java.lang.String ns) {
    this.ns = ns;
    return this;
  }

  public void unsetNs() {
    this.ns = null;
  }

  /** Returns true if field ns is set (has been assigned a value) and false otherwise */
  public boolean isSetNs() {
    return this.ns != null;
  }

  public void setNsIsSet(boolean value) {
    if (!value) {
      this.ns = null;
    }
  }

  public int getReadValueListSize() {
    return (this.readValueList == null) ? 0 : this.readValueList.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<ReadListValue> getReadValueListIterator() {
    return (this.readValueList == null) ? null : this.readValueList.iterator();
  }

  public void addToReadValueList(ReadListValue elem) {
    if (this.readValueList == null) {
      this.readValueList = new java.util.ArrayList<ReadListValue>();
    }
    this.readValueList.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<ReadListValue> getReadValueList() {
    return this.readValueList;
  }

  public GetListRequest setReadValueList(@org.apache.thrift.annotation.Nullable java.util.List<ReadListValue> readValueList) {
    this.readValueList = readValueList;
    return this;
  }

  public void unsetReadValueList() {
    this.readValueList = null;
  }

  /** Returns true if field readValueList is set (has been assigned a value) and false otherwise */
  public boolean isSetReadValueList() {
    return this.readValueList != null;
  }

  public void setReadValueListIsSet(boolean value) {
    if (!value) {
      this.readValueList = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public org.apache.flink.connectors.loghouse.service.Base getBase() {
    return this.Base;
  }

  public GetListRequest setBase(@org.apache.thrift.annotation.Nullable org.apache.flink.connectors.loghouse.service.Base Base) {
    this.Base = Base;
    return this;
  }

  public void unsetBase() {
    this.Base = null;
  }

  /** Returns true if field Base is set (has been assigned a value) and false otherwise */
  public boolean isSetBase() {
    return this.Base != null;
  }

  public void setBaseIsSet(boolean value) {
    if (!value) {
      this.Base = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case NS:
      if (value == null) {
        unsetNs();
      } else {
        setNs((java.lang.String)value);
      }
      break;

    case READ_VALUE_LIST:
      if (value == null) {
        unsetReadValueList();
      } else {
        setReadValueList((java.util.List<ReadListValue>)value);
      }
      break;

    case BASE:
      if (value == null) {
        unsetBase();
      } else {
        setBase((org.apache.flink.connectors.loghouse.service.Base)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case NS:
      return getNs();

    case READ_VALUE_LIST:
      return getReadValueList();

    case BASE:
      return getBase();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case NS:
      return isSetNs();
    case READ_VALUE_LIST:
      return isSetReadValueList();
    case BASE:
      return isSetBase();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof GetListRequest)
      return this.equals((GetListRequest)that);
    return false;
  }

  public boolean equals(GetListRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_ns = true && this.isSetNs();
    boolean that_present_ns = true && that.isSetNs();
    if (this_present_ns || that_present_ns) {
      if (!(this_present_ns && that_present_ns))
        return false;
      if (!this.ns.equals(that.ns))
        return false;
    }

    boolean this_present_readValueList = true && this.isSetReadValueList();
    boolean that_present_readValueList = true && that.isSetReadValueList();
    if (this_present_readValueList || that_present_readValueList) {
      if (!(this_present_readValueList && that_present_readValueList))
        return false;
      if (!this.readValueList.equals(that.readValueList))
        return false;
    }

    boolean this_present_Base = true && this.isSetBase();
    boolean that_present_Base = true && that.isSetBase();
    if (this_present_Base || that_present_Base) {
      if (!(this_present_Base && that_present_Base))
        return false;
      if (!this.Base.equals(that.Base))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetNs()) ? 131071 : 524287);
    if (isSetNs())
      hashCode = hashCode * 8191 + ns.hashCode();

    hashCode = hashCode * 8191 + ((isSetReadValueList()) ? 131071 : 524287);
    if (isSetReadValueList())
      hashCode = hashCode * 8191 + readValueList.hashCode();

    hashCode = hashCode * 8191 + ((isSetBase()) ? 131071 : 524287);
    if (isSetBase())
      hashCode = hashCode * 8191 + Base.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(GetListRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetNs()).compareTo(other.isSetNs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ns, other.ns);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetReadValueList()).compareTo(other.isSetReadValueList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetReadValueList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.readValueList, other.readValueList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetBase()).compareTo(other.isSetBase());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBase()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.Base, other.Base);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("GetListRequest(");
    boolean first = true;

    sb.append("ns:");
    if (this.ns == null) {
      sb.append("null");
    } else {
      sb.append(this.ns);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("readValueList:");
    if (this.readValueList == null) {
      sb.append("null");
    } else {
      sb.append(this.readValueList);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("Base:");
    if (this.Base == null) {
      sb.append("null");
    } else {
      sb.append(this.Base);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (Base != null) {
      Base.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetListRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public GetListRequestStandardScheme getScheme() {
      return new GetListRequestStandardScheme();
    }
  }

  private static class GetListRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<GetListRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetListRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.ns = iprot.readString();
              struct.setNsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // READ_VALUE_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list42 = iprot.readListBegin();
                struct.readValueList = new java.util.ArrayList<ReadListValue>(_list42.size);
                @org.apache.thrift.annotation.Nullable ReadListValue _elem43;
                for (int _i44 = 0; _i44 < _list42.size; ++_i44)
                {
                  _elem43 = new ReadListValue();
                  _elem43.read(iprot);
                  struct.readValueList.add(_elem43);
                }
                iprot.readListEnd();
              }
              struct.setReadValueListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 255: // BASE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.Base = new org.apache.flink.connectors.loghouse.service.Base();
              struct.Base.read(iprot);
              struct.setBaseIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetListRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.ns != null) {
        oprot.writeFieldBegin(NS_FIELD_DESC);
        oprot.writeString(struct.ns);
        oprot.writeFieldEnd();
      }
      if (struct.readValueList != null) {
        oprot.writeFieldBegin(READ_VALUE_LIST_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.readValueList.size()));
          for (ReadListValue _iter45 : struct.readValueList)
          {
            _iter45.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.Base != null) {
        oprot.writeFieldBegin(BASE_FIELD_DESC);
        struct.Base.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetListRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public GetListRequestTupleScheme getScheme() {
      return new GetListRequestTupleScheme();
    }
  }

  private static class GetListRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<GetListRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetListRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetNs()) {
        optionals.set(0);
      }
      if (struct.isSetReadValueList()) {
        optionals.set(1);
      }
      if (struct.isSetBase()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetNs()) {
        oprot.writeString(struct.ns);
      }
      if (struct.isSetReadValueList()) {
        {
          oprot.writeI32(struct.readValueList.size());
          for (ReadListValue _iter46 : struct.readValueList)
          {
            _iter46.write(oprot);
          }
        }
      }
      if (struct.isSetBase()) {
        struct.Base.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetListRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.ns = iprot.readString();
        struct.setNsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list47 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.readValueList = new java.util.ArrayList<ReadListValue>(_list47.size);
          @org.apache.thrift.annotation.Nullable ReadListValue _elem48;
          for (int _i49 = 0; _i49 < _list47.size; ++_i49)
          {
            _elem48 = new ReadListValue();
            _elem48.read(iprot);
            struct.readValueList.add(_elem48);
          }
        }
        struct.setReadValueListIsSet(true);
      }
      if (incoming.get(2)) {
        struct.Base = new org.apache.flink.connectors.loghouse.service.Base();
        struct.Base.read(iprot);
        struct.setBaseIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
