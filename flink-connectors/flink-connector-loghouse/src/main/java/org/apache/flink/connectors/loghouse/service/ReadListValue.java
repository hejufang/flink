/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.flink.connectors.loghouse.service;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-03-10")
public class ReadListValue implements org.apache.thrift.TBase<ReadListValue, ReadListValue._Fields>, java.io.Serializable, Cloneable, Comparable<ReadListValue> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReadListValue");

  private static final org.apache.thrift.protocol.TField HDFS_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("hdfsPath", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField KEY_OFFSET_MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("keyOffsetMap", org.apache.thrift.protocol.TType.MAP, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ReadListValueStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ReadListValueTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String hdfsPath; // required
  public @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> keyOffsetMap; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HDFS_PATH((short)1, "hdfsPath"),
    KEY_OFFSET_MAP((short)2, "keyOffsetMap");

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
        case 1: // HDFS_PATH
          return HDFS_PATH;
        case 2: // KEY_OFFSET_MAP
          return KEY_OFFSET_MAP;
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
    tmpMap.put(_Fields.HDFS_PATH, new org.apache.thrift.meta_data.FieldMetaData("hdfsPath", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.KEY_OFFSET_MAP, new org.apache.thrift.meta_data.FieldMetaData("keyOffsetMap", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReadListValue.class, metaDataMap);
  }

  public ReadListValue() {
  }

  public ReadListValue(
    java.lang.String hdfsPath,
    java.util.Map<java.lang.String,java.lang.String> keyOffsetMap)
  {
    this();
    this.hdfsPath = hdfsPath;
    this.keyOffsetMap = keyOffsetMap;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReadListValue(ReadListValue other) {
    if (other.isSetHdfsPath()) {
      this.hdfsPath = other.hdfsPath;
    }
    if (other.isSetKeyOffsetMap()) {
      java.util.Map<java.lang.String,java.lang.String> __this__keyOffsetMap = new java.util.HashMap<java.lang.String,java.lang.String>(other.keyOffsetMap);
      this.keyOffsetMap = __this__keyOffsetMap;
    }
  }

  public ReadListValue deepCopy() {
    return new ReadListValue(this);
  }

  @Override
  public void clear() {
    this.hdfsPath = null;
    this.keyOffsetMap = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getHdfsPath() {
    return this.hdfsPath;
  }

  public ReadListValue setHdfsPath(@org.apache.thrift.annotation.Nullable java.lang.String hdfsPath) {
    this.hdfsPath = hdfsPath;
    return this;
  }

  public void unsetHdfsPath() {
    this.hdfsPath = null;
  }

  /** Returns true if field hdfsPath is set (has been assigned a value) and false otherwise */
  public boolean isSetHdfsPath() {
    return this.hdfsPath != null;
  }

  public void setHdfsPathIsSet(boolean value) {
    if (!value) {
      this.hdfsPath = null;
    }
  }

  public int getKeyOffsetMapSize() {
    return (this.keyOffsetMap == null) ? 0 : this.keyOffsetMap.size();
  }

  public void putToKeyOffsetMap(java.lang.String key, java.lang.String val) {
    if (this.keyOffsetMap == null) {
      this.keyOffsetMap = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.keyOffsetMap.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getKeyOffsetMap() {
    return this.keyOffsetMap;
  }

  public ReadListValue setKeyOffsetMap(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> keyOffsetMap) {
    this.keyOffsetMap = keyOffsetMap;
    return this;
  }

  public void unsetKeyOffsetMap() {
    this.keyOffsetMap = null;
  }

  /** Returns true if field keyOffsetMap is set (has been assigned a value) and false otherwise */
  public boolean isSetKeyOffsetMap() {
    return this.keyOffsetMap != null;
  }

  public void setKeyOffsetMapIsSet(boolean value) {
    if (!value) {
      this.keyOffsetMap = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case HDFS_PATH:
      if (value == null) {
        unsetHdfsPath();
      } else {
        setHdfsPath((java.lang.String)value);
      }
      break;

    case KEY_OFFSET_MAP:
      if (value == null) {
        unsetKeyOffsetMap();
      } else {
        setKeyOffsetMap((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case HDFS_PATH:
      return getHdfsPath();

    case KEY_OFFSET_MAP:
      return getKeyOffsetMap();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case HDFS_PATH:
      return isSetHdfsPath();
    case KEY_OFFSET_MAP:
      return isSetKeyOffsetMap();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ReadListValue)
      return this.equals((ReadListValue)that);
    return false;
  }

  public boolean equals(ReadListValue that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_hdfsPath = true && this.isSetHdfsPath();
    boolean that_present_hdfsPath = true && that.isSetHdfsPath();
    if (this_present_hdfsPath || that_present_hdfsPath) {
      if (!(this_present_hdfsPath && that_present_hdfsPath))
        return false;
      if (!this.hdfsPath.equals(that.hdfsPath))
        return false;
    }

    boolean this_present_keyOffsetMap = true && this.isSetKeyOffsetMap();
    boolean that_present_keyOffsetMap = true && that.isSetKeyOffsetMap();
    if (this_present_keyOffsetMap || that_present_keyOffsetMap) {
      if (!(this_present_keyOffsetMap && that_present_keyOffsetMap))
        return false;
      if (!this.keyOffsetMap.equals(that.keyOffsetMap))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetHdfsPath()) ? 131071 : 524287);
    if (isSetHdfsPath())
      hashCode = hashCode * 8191 + hdfsPath.hashCode();

    hashCode = hashCode * 8191 + ((isSetKeyOffsetMap()) ? 131071 : 524287);
    if (isSetKeyOffsetMap())
      hashCode = hashCode * 8191 + keyOffsetMap.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ReadListValue other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetHdfsPath()).compareTo(other.isSetHdfsPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHdfsPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hdfsPath, other.hdfsPath);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetKeyOffsetMap()).compareTo(other.isSetKeyOffsetMap());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeyOffsetMap()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keyOffsetMap, other.keyOffsetMap);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ReadListValue(");
    boolean first = true;

    sb.append("hdfsPath:");
    if (this.hdfsPath == null) {
      sb.append("null");
    } else {
      sb.append(this.hdfsPath);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("keyOffsetMap:");
    if (this.keyOffsetMap == null) {
      sb.append("null");
    } else {
      sb.append(this.keyOffsetMap);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
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

  private static class ReadListValueStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadListValueStandardScheme getScheme() {
      return new ReadListValueStandardScheme();
    }
  }

  private static class ReadListValueStandardScheme extends org.apache.thrift.scheme.StandardScheme<ReadListValue> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReadListValue struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // HDFS_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.hdfsPath = iprot.readString();
              struct.setHdfsPathIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // KEY_OFFSET_MAP
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map0 = iprot.readMapBegin();
                struct.keyOffsetMap = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map0.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key1;
                @org.apache.thrift.annotation.Nullable java.lang.String _val2;
                for (int _i3 = 0; _i3 < _map0.size; ++_i3)
                {
                  _key1 = iprot.readString();
                  _val2 = iprot.readString();
                  struct.keyOffsetMap.put(_key1, _val2);
                }
                iprot.readMapEnd();
              }
              struct.setKeyOffsetMapIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReadListValue struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.hdfsPath != null) {
        oprot.writeFieldBegin(HDFS_PATH_FIELD_DESC);
        oprot.writeString(struct.hdfsPath);
        oprot.writeFieldEnd();
      }
      if (struct.keyOffsetMap != null) {
        oprot.writeFieldBegin(KEY_OFFSET_MAP_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.keyOffsetMap.size()));
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter4 : struct.keyOffsetMap.entrySet())
          {
            oprot.writeString(_iter4.getKey());
            oprot.writeString(_iter4.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReadListValueTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadListValueTupleScheme getScheme() {
      return new ReadListValueTupleScheme();
    }
  }

  private static class ReadListValueTupleScheme extends org.apache.thrift.scheme.TupleScheme<ReadListValue> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReadListValue struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetHdfsPath()) {
        optionals.set(0);
      }
      if (struct.isSetKeyOffsetMap()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetHdfsPath()) {
        oprot.writeString(struct.hdfsPath);
      }
      if (struct.isSetKeyOffsetMap()) {
        {
          oprot.writeI32(struct.keyOffsetMap.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter5 : struct.keyOffsetMap.entrySet())
          {
            oprot.writeString(_iter5.getKey());
            oprot.writeString(_iter5.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReadListValue struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.hdfsPath = iprot.readString();
        struct.setHdfsPathIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map6 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.keyOffsetMap = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map6.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key7;
          @org.apache.thrift.annotation.Nullable java.lang.String _val8;
          for (int _i9 = 0; _i9 < _map6.size; ++_i9)
          {
            _key7 = iprot.readString();
            _val8 = iprot.readString();
            struct.keyOffsetMap.put(_key7, _val8);
          }
        }
        struct.setKeyOffsetMapIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

