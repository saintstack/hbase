/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hbase.thrift2.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * Atomic mutation for the specified row. It can be either Put or Delete.
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-04-14")
public class TMutation extends org.apache.thrift.TUnion<TMutation, TMutation._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TMutation");
  private static final org.apache.thrift.protocol.TField PUT_FIELD_DESC = new org.apache.thrift.protocol.TField("put", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField DELETE_SINGLE_FIELD_DESC = new org.apache.thrift.protocol.TField("deleteSingle", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PUT((short)1, "put"),
    DELETE_SINGLE((short)2, "deleteSingle");

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
        case 1: // PUT
          return PUT;
        case 2: // DELETE_SINGLE
          return DELETE_SINGLE;
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

  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PUT, new org.apache.thrift.meta_data.FieldMetaData("put", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TPut.class)));
    tmpMap.put(_Fields.DELETE_SINGLE, new org.apache.thrift.meta_data.FieldMetaData("deleteSingle", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TDelete.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TMutation.class, metaDataMap);
  }

  public TMutation() {
    super();
  }

  public TMutation(_Fields setField, java.lang.Object value) {
    super(setField, value);
  }

  public TMutation(TMutation other) {
    super(other);
  }
  public TMutation deepCopy() {
    return new TMutation(this);
  }

  public static TMutation put(TPut value) {
    TMutation x = new TMutation();
    x.setPut(value);
    return x;
  }

  public static TMutation deleteSingle(TDelete value) {
    TMutation x = new TMutation();
    x.setDeleteSingle(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, java.lang.Object value) throws java.lang.ClassCastException {
    switch (setField) {
      case PUT:
        if (value instanceof TPut) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type TPut for field 'put', but got " + value.getClass().getSimpleName());
      case DELETE_SINGLE:
        if (value instanceof TDelete) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type TDelete for field 'deleteSingle', but got " + value.getClass().getSimpleName());
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected java.lang.Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case PUT:
          if (field.type == PUT_FIELD_DESC.type) {
            TPut put;
            put = new TPut();
            put.read(iprot);
            return put;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DELETE_SINGLE:
          if (field.type == DELETE_SINGLE_FIELD_DESC.type) {
            TDelete deleteSingle;
            deleteSingle = new TDelete();
            deleteSingle.read(iprot);
            return deleteSingle;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case PUT:
        TPut put = (TPut)value_;
        put.write(oprot);
        return;
      case DELETE_SINGLE:
        TDelete deleteSingle = (TDelete)value_;
        deleteSingle.write(oprot);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected java.lang.Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case PUT:
          TPut put;
          put = new TPut();
          put.read(iprot);
          return put;
        case DELETE_SINGLE:
          TDelete deleteSingle;
          deleteSingle = new TDelete();
          deleteSingle.read(iprot);
          return deleteSingle;
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new org.apache.thrift.protocol.TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case PUT:
        TPut put = (TPut)value_;
        put.write(oprot);
        return;
      case DELETE_SINGLE:
        TDelete deleteSingle = (TDelete)value_;
        deleteSingle.write(oprot);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case PUT:
        return PUT_FIELD_DESC;
      case DELETE_SINGLE:
        return DELETE_SINGLE_FIELD_DESC;
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public TPut getPut() {
    if (getSetField() == _Fields.PUT) {
      return (TPut)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'put' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setPut(TPut value) {
    if (value == null) throw new java.lang.NullPointerException();
    setField_ = _Fields.PUT;
    value_ = value;
  }

  public TDelete getDeleteSingle() {
    if (getSetField() == _Fields.DELETE_SINGLE) {
      return (TDelete)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'deleteSingle' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDeleteSingle(TDelete value) {
    if (value == null) throw new java.lang.NullPointerException();
    setField_ = _Fields.DELETE_SINGLE;
    value_ = value;
  }

  public boolean isSetPut() {
    return setField_ == _Fields.PUT;
  }


  public boolean isSetDeleteSingle() {
    return setField_ == _Fields.DELETE_SINGLE;
  }


  public boolean equals(java.lang.Object other) {
    if (other instanceof TMutation) {
      return equals((TMutation)other);
    } else {
      return false;
    }
  }

  public boolean equals(TMutation other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(TMutation other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    java.util.List<java.lang.Object> list = new java.util.ArrayList<java.lang.Object>();
    list.add(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      list.add(setField.getThriftFieldId());
      java.lang.Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        list.add(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        list.add(value);
      }
    }
    return list.hashCode();
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


}
