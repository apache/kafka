/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/



package org.apache.kafka.copycat.data;

import java.nio.ByteBuffer;
import java.util.*;

/** An abstract data type.
 * <p>A schema may be one of:
 * <ul>
 * <li>A <i>record</i>, mapping field names to field value data;
 * <li>An <i>enum</i>, containing one of a small set of symbols;
 * <li>An <i>array</i> of values, all of the same schema;
 * <li>A <i>map</i>, containing string/value pairs, of a declared schema;
 * <li>A <i>union</i> of other schemas;
 * <li>A <i>fixed</i> sized binary object;
 * <li>A unicode <i>string</i>;
 * <li>A sequence of <i>bytes</i>;
 * <li>A 32-bit signed <i>int</i>;
 * <li>A 64-bit signed <i>long</i>;
 * <li>A 32-bit IEEE single-<i>float</i>; or
 * <li>A 64-bit IEEE <i>double</i>-float; or
 * <li>A <i>boolean</i>; or
 * <li><i>null</i>.
 * </ul>
 *
 * A schema can be constructed using one of its static <tt>createXXX</tt>
 * methods, or more conveniently using {@link SchemaBuilder}. The schema objects are
 * <i>logically</i> immutable.
 * There are only two mutating methods - {@link #setFields(List)} and
 * {@link #addProp(String, Object)}. The following restrictions apply on these
 * two methods.
 * <ul>
 * <li> {@link #setFields(List)}, can be called at most once. This method exists
 * in order to enable clients to build recursive schemas.
 * <li> {@link #addProp(String, Object)} can be called with property names
 * that are not present already. It is not possible to change or delete an
 * existing property.
 * </ul>
 */
public abstract class Schema extends ObjectProperties {
    private static final int NO_HASHCODE = Integer.MIN_VALUE;

    /** The type of a schema. */
    public enum Type {
        ENUM {
            @Override
            public Object defaultValue(Schema schema) {
                return null;
            }
        },
        ARRAY {
            @Override
            public Object defaultValue(Schema schema) {
                return new ArrayList<>();
            }
        },
        MAP {
            @Override
            public Object defaultValue(Schema schema) {
                return new HashMap<Object, Object>();
            }
        },
        UNION {
            @Override
            public Object defaultValue(Schema schema) {
                Schema firstSchema = schema.getTypes().get(0);
                return firstSchema.getType().defaultValue(firstSchema);
            }
        },
        STRING {
            @Override
            public Object defaultValue(Schema schema) {
                return "";
            }
        },
        BYTES {
            @Override
            public Object defaultValue(Schema schema) {
                return new byte[0];
            }
        },
        INT {
            @Override
            public Object defaultValue(Schema schema) {
                return 0;
            }
        },
        LONG {
            @Override
            public Object defaultValue(Schema schema) {
                return 0;
            }
        },
        FLOAT {
            @Override
            public Object defaultValue(Schema schema) {
                return 0;
            }
        },
        DOUBLE {
            @Override
            public Object defaultValue(Schema schema) {
                return 0;
            }
        },
        BOOLEAN {
            @Override
            public Object defaultValue(Schema schema) {
                return false;
            }
        },
        NULL {
            @Override
            public Object defaultValue(Schema schema) {
                return null;
            }
        };
        private String name;

        private Type() {
            this.name = this.name().toLowerCase();
        }

        public String getName() {
            return name;
        }

        public abstract Object defaultValue(Schema schema);
    }

    private final Type type;

    Schema(Type type) {
        super(SCHEMA_RESERVED);
        this.type = type;
    }

    /** Create a schema for a primitive type. */
    public static Schema create(Type type) {
        switch (type) {
            case STRING:
                return new StringSchema();
            case BYTES:
                return new BytesSchema();
            case INT:
                return new IntSchema();
            case LONG:
                return new LongSchema();
            case FLOAT:
                return new FloatSchema();
            case DOUBLE:
                return new DoubleSchema();
            case BOOLEAN:
                return new BooleanSchema();
            case NULL:
                return new NullSchema();
            default:
                throw new DataRuntimeException("Can't create a: " + type);
        }
    }

    private static final Set<String> SCHEMA_RESERVED = new HashSet<String>();

    static {
        Collections.addAll(SCHEMA_RESERVED,
                "doc", "fields", "items", "name", "namespace",
                "size", "symbols", "values", "type", "aliases");
    }

    int hashCode = NO_HASHCODE;

    @Override
    public void addProp(String name, Object value) {
        super.addProp(name, value);
        hashCode = NO_HASHCODE;
    }

    /** Create an enum schema. */
    public static Schema createEnum(String name, String doc, String namespace,
                                    List<String> values) {
        return new EnumSchema(new Name(name, namespace), doc,
                new LockableArrayList<String>(values));
    }

    /** Create an array schema. */
    public static Schema createArray(Schema elementType) {
        return new ArraySchema(elementType);
    }

    /** Create a map schema. */
    public static Schema createMap(Schema valueType) {
        return new MapSchema(valueType);
    }

    /** Create a union schema. */
    public static Schema createUnion(List<Schema> types) {
        return new UnionSchema(new LockableArrayList<Schema>(types));
    }

    /** Create a union schema. */
    public static Schema createUnion(Schema... types) {
        return createUnion(new LockableArrayList<Schema>(types));
    }

    /** Return the type of this schema. */
    public Type getType() {
        return type;
    }

    /**
     * If this is a record, returns the Field with the
     * given name <tt>fieldName</tt>. If there is no field by that name, a
     * <tt>null</tt> is returned.
     */
    public Field getField(String fieldname) {
        throw new DataRuntimeException("Not a record: " + this);
    }

    /**
     * If this is a record, returns the fields in it. The returned
     * list is in the order of their positions.
     */
    public List<Field> getFields() {
        throw new DataRuntimeException("Not a record: " + this);
    }

    /**
     * If this is a record, set its fields. The fields can be set
     * only once in a schema.
     */
    public void setFields(List<Field> fields) {
        throw new DataRuntimeException("Not a record: " + this);
    }

    /** If this is an enum, return its symbols. */
    public List<String> getEnumSymbols() {
        throw new DataRuntimeException("Not an enum: " + this);
    }

    /** If this is an enum, return a symbol's ordinal value. */
    public int getEnumOrdinal(String symbol) {
        throw new DataRuntimeException("Not an enum: " + this);
    }

    /** If this is an enum, returns true if it contains given symbol. */
    public boolean hasEnumSymbol(String symbol) {
        throw new DataRuntimeException("Not an enum: " + this);
    }

    /** If this is a record, enum or fixed, returns its name, otherwise the name
     * of the primitive type. */
    public String getName() {
        return type.name;
    }

    /** If this is a record, enum, or fixed, returns its docstring,
     * if available.  Otherwise, returns null. */
    public String getDoc() {
        return null;
    }

    /** If this is a record, enum or fixed, returns its namespace, if any. */
    public String getNamespace() {
        throw new DataRuntimeException("Not a named type: " + this);
    }

    /** If this is a record, enum or fixed, returns its namespace-qualified name,
     * otherwise returns the name of the primitive type. */
    public String getFullName() {
        return getName();
    }

    /** If this is a record, enum or fixed, add an alias. */
    public void addAlias(String alias) {
        throw new DataRuntimeException("Not a named type: " + this);
    }

    /** If this is a record, enum or fixed, add an alias. */
    public void addAlias(String alias, String space) {
        throw new DataRuntimeException("Not a named type: " + this);
    }

    /** If this is a record, enum or fixed, return its aliases, if any. */
    public Set<String> getAliases() {
        throw new DataRuntimeException("Not a named type: " + this);
    }

    /** Returns true if this record is an error type. */
    public boolean isError() {
        throw new DataRuntimeException("Not a record: " + this);
    }

    /** If this is an array, returns its element type. */
    public Schema getElementType() {
        throw new DataRuntimeException("Not an array: " + this);
    }

    /** If this is a map, returns its value type. */
    public Schema getValueType() {
        throw new DataRuntimeException("Not a map: " + this);
    }

    /** If this is a union, returns its types. */
    public List<Schema> getTypes() {
        throw new DataRuntimeException("Not a union: " + this);
    }

    /** If this is a union, return the branch with the provided full name. */
    public Integer getIndexNamed(String name) {
        throw new DataRuntimeException("Not a union: " + this);
    }

    /** If this is fixed, returns its size. */
    public int getFixedSize() {
        throw new DataRuntimeException("Not fixed: " + this);
    }

    @Override
    public String toString() {
        // FIXME A more JSON-like output showing the details would be nice
        return "Schema:" + this.getType() + ":" + getFullName();
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Schema)) return false;
        Schema that = (Schema) o;
        if (!(this.type == that.type)) return false;
        return equalCachedHash(that) && props.equals(that.props);
    }

    public final int hashCode() {
        if (hashCode == NO_HASHCODE)
            hashCode = computeHash();
        return hashCode;
    }

    int computeHash() {
        return getType().hashCode() + props.hashCode();
    }

    final boolean equalCachedHash(Schema other) {
        return (hashCode == other.hashCode)
                || (hashCode == NO_HASHCODE)
                || (other.hashCode == NO_HASHCODE);
    }

    private static final Set<String> FIELD_RESERVED = new HashSet<String>();

    static {
        Collections.addAll(FIELD_RESERVED,
                "default", "doc", "name", "order", "type", "aliases");
    }

    /** A field within a record. */
    public static class Field extends ObjectProperties {

        /** How values of this field should be ordered when sorting records. */
        public enum Order {
            ASCENDING, DESCENDING, IGNORE;
            private String name;

            private Order() {
                this.name = this.name().toLowerCase();
            }
        }


        private final String name;    // name of the field.
        private int position = -1;
        private final Schema schema;
        private final String doc;
        private final Object defaultValue;
        private final Order order;
        private Set<String> aliases;

        public Field(String name, Schema schema, String doc,
                     Object defaultValue) {
            this(name, schema, doc, defaultValue, Order.ASCENDING);
        }

        public Field(String name, Schema schema, String doc,
                     Object defaultValue, Order order) {
            super(FIELD_RESERVED);
            this.name = validateName(name);
            this.schema = schema;
            this.doc = doc;
            this.defaultValue = validateDefault(name, schema, defaultValue);
            this.order = order;
        }

        public String name() {
            return name;
        }


        /** The position of this field within the record. */
        public int pos() {
            return position;
        }

        /** This field's {@link Schema}. */
        public Schema schema() {
            return schema;
        }

        /** Field's documentation within the record, if set. May return null. */
        public String doc() {
            return doc;
        }

        public Object defaultValue() {
            return defaultValue;
        }

        public Order order() {
            return order;
        }

        public void addAlias(String alias) {
            if (aliases == null)
                this.aliases = new LinkedHashSet<String>();
            aliases.add(alias);
        }

        /** Return the defined aliases as an unmodifieable Set. */
        public Set<String> aliases() {
            if (aliases == null)
                return Collections.emptySet();
            return Collections.unmodifiableSet(aliases);
        }

        public boolean equals(Object other) {
            if (other == this) return true;
            if (!(other instanceof Field)) return false;
            Field that = (Field) other;
            return (name.equals(that.name)) &&
                    (schema.equals(that.schema)) &&
                    defaultValueEquals(that.defaultValue) &&
                    (order == that.order) &&
                    props.equals(that.props);
        }

        public int hashCode() {
            return name.hashCode() + schema.computeHash();
        }

        /** Do any possible implicit conversions to double, or return 0 if there isn't a
         * valid conversion */
        private double doubleValue(Object v) {
            if (v instanceof Integer)
                return (double) (Integer) v;
            else if (v instanceof Long)
                return (double) (Long) v;
            else if (v instanceof Float)
                return (double) (Float) v;
            else if (v instanceof Double)
                return (double) (Double) v;
            else
                return 0;
        }

        private boolean defaultValueEquals(Object thatDefaultValue) {
            if (defaultValue == null)
                return thatDefaultValue == null;
            if (Double.isNaN(doubleValue(defaultValue)))
                return Double.isNaN(doubleValue(thatDefaultValue));
            return defaultValue.equals(thatDefaultValue);
        }

        @Override
        public String toString() {
            return name + " type:" + schema.type + " pos:" + position;
        }
    }

    static class Name {
        private final String name;
        private final String space;
        private final String full;

        public Name(String name, String space) {
            if (name == null) {                         // anonymous
                this.name = this.space = this.full = null;
                return;
            }
            int lastDot = name.lastIndexOf('.');
            if (lastDot < 0) {                          // unqualified name
                this.name = validateName(name);
            } else {                                    // qualified name
                space = name.substring(0, lastDot);       // get space from name
                this.name = validateName(name.substring(lastDot + 1, name.length()));
            }
            if ("".equals(space))
                space = null;
            this.space = space;
            this.full = (this.space == null) ? this.name : this.space + "." + this.name;
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Name)) return false;
            Name that = (Name) o;
            return full == null ? that.full == null : full.equals(that.full);
        }

        public int hashCode() {
            return full == null ? 0 : full.hashCode();
        }

        public String toString() {
            return full;
        }

        public String getQualified(String defaultSpace) {
            return (space == null || space.equals(defaultSpace)) ? name : full;
        }
    }

    private static abstract class NamedSchema extends Schema {
        final Name name;
        final String doc;
        Set<Name> aliases;

        public NamedSchema(Type type, Name name, String doc) {
            super(type);
            this.name = name;
            this.doc = doc;
            if (PRIMITIVES.containsKey(name.full)) {
                throw new DataTypeException("Schemas may not be named after primitives: " + name.full);
            }
        }

        public String getName() {
            return name.name;
        }

        public String getDoc() {
            return doc;
        }

        public String getNamespace() {
            return name.space;
        }

        public String getFullName() {
            return name.full;
        }

        public void addAlias(String alias) {
            addAlias(alias, null);
        }

        public void addAlias(String name, String space) {
            if (aliases == null)
                this.aliases = new LinkedHashSet<Name>();
            if (space == null)
                space = this.name.space;
            aliases.add(new Name(name, space));
        }

        public Set<String> getAliases() {
            Set<String> result = new LinkedHashSet<String>();
            if (aliases != null)
                for (Name alias : aliases)
                    result.add(alias.full);
            return result;
        }

        public boolean equalNames(NamedSchema that) {
            return this.name.equals(that.name);
        }

        @Override
        int computeHash() {
            return super.computeHash() + name.hashCode();
        }
    }

    private static class SeenPair {
        private Object s1;
        private Object s2;

        private SeenPair(Object s1, Object s2) {
            this.s1 = s1;
            this.s2 = s2;
        }

        public boolean equals(Object o) {
            return this.s1 == ((SeenPair) o).s1 && this.s2 == ((SeenPair) o).s2;
        }

        public int hashCode() {
            return System.identityHashCode(s1) + System.identityHashCode(s2);
        }
    }

    private static final ThreadLocal<Set> SEEN_EQUALS = new ThreadLocal<Set>() {
        protected Set initialValue() {
            return new HashSet();
        }
    };
    private static final ThreadLocal<Map> SEEN_HASHCODE = new ThreadLocal<Map>() {
        protected Map initialValue() {
            return new IdentityHashMap();
        }
    };

    private static class EnumSchema extends NamedSchema {
        private final List<String> symbols;
        private final Map<String, Integer> ordinals;

        public EnumSchema(Name name, String doc,
                          LockableArrayList<String> symbols) {
            super(Type.ENUM, name, doc);
            this.symbols = symbols.lock();
            this.ordinals = new HashMap<String, Integer>();
            int i = 0;
            for (String symbol : symbols)
                if (ordinals.put(validateName(symbol), i++) != null)
                    throw new SchemaParseException("Duplicate enum symbol: " + symbol);
        }

        public List<String> getEnumSymbols() {
            return symbols;
        }

        public boolean hasEnumSymbol(String symbol) {
            return ordinals.containsKey(symbol);
        }

        public int getEnumOrdinal(String symbol) {
            return ordinals.get(symbol);
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof EnumSchema)) return false;
            EnumSchema that = (EnumSchema) o;
            return equalCachedHash(that)
                    && equalNames(that)
                    && symbols.equals(that.symbols)
                    && props.equals(that.props);
        }

        @Override
        int computeHash() {
            return super.computeHash() + symbols.hashCode();
        }
    }

    private static class ArraySchema extends Schema {
        private final Schema elementType;

        public ArraySchema(Schema elementType) {
            super(Type.ARRAY);
            this.elementType = elementType;
        }

        public Schema getElementType() {
            return elementType;
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof ArraySchema)) return false;
            ArraySchema that = (ArraySchema) o;
            return equalCachedHash(that)
                    && elementType.equals(that.elementType)
                    && props.equals(that.props);
        }

        @Override
        int computeHash() {
            return super.computeHash() + elementType.computeHash();
        }
    }

    private static class MapSchema extends Schema {
        private final Schema valueType;

        public MapSchema(Schema valueType) {
            super(Type.MAP);
            this.valueType = valueType;
        }

        public Schema getValueType() {
            return valueType;
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof MapSchema)) return false;
            MapSchema that = (MapSchema) o;
            return equalCachedHash(that)
                    && valueType.equals(that.valueType)
                    && props.equals(that.props);
        }

        @Override
        int computeHash() {
            return super.computeHash() + valueType.computeHash();
        }
    }

    private static class UnionSchema extends Schema {
        private final List<Schema> types;
        private final Map<String, Integer> indexByName
                = new HashMap<String, Integer>();

        public UnionSchema(LockableArrayList<Schema> types) {
            super(Type.UNION);
            this.types = types.lock();
            int index = 0;
            for (Schema type : types) {
                if (type.getType() == Type.UNION)
                    throw new DataRuntimeException("Nested union: " + this);
                String name = type.getFullName();
                if (name == null)
                    throw new DataRuntimeException("Nameless in union:" + this);
                if (indexByName.put(name, index++) != null)
                    throw new DataRuntimeException("Duplicate in union:" + name);
            }
        }

        public List<Schema> getTypes() {
            return types;
        }

        public Integer getIndexNamed(String name) {
            return indexByName.get(name);
        }

        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof UnionSchema)) return false;
            UnionSchema that = (UnionSchema) o;
            return equalCachedHash(that)
                    && types.equals(that.types)
                    && props.equals(that.props);
        }

        @Override
        int computeHash() {
            int hash = super.computeHash();
            for (Schema type : types)
                hash += type.computeHash();
            return hash;
        }
    }

    private static class StringSchema extends Schema {
        public StringSchema() {
            super(Type.STRING);
        }
    }

    private static class BytesSchema extends Schema {
        public BytesSchema() {
            super(Type.BYTES);
        }
    }

    private static class IntSchema extends Schema {
        public IntSchema() {
            super(Type.INT);
        }
    }

    private static class LongSchema extends Schema {
        public LongSchema() {
            super(Type.LONG);
        }
    }

    private static class FloatSchema extends Schema {
        public FloatSchema() {
            super(Type.FLOAT);
        }
    }

    private static class DoubleSchema extends Schema {
        public DoubleSchema() {
            super(Type.DOUBLE);
        }
    }

    private static class BooleanSchema extends Schema {
        public BooleanSchema() {
            super(Type.BOOLEAN);
        }
    }

    private static class NullSchema extends Schema {
        public NullSchema() {
            super(Type.NULL);
        }
    }

    static final Map<String, Type> PRIMITIVES = new HashMap<String, Type>();

    static {
        PRIMITIVES.put("string", Type.STRING);
        PRIMITIVES.put("bytes", Type.BYTES);
        PRIMITIVES.put("int", Type.INT);
        PRIMITIVES.put("long", Type.LONG);
        PRIMITIVES.put("float", Type.FLOAT);
        PRIMITIVES.put("double", Type.DOUBLE);
        PRIMITIVES.put("boolean", Type.BOOLEAN);
        PRIMITIVES.put("null", Type.NULL);
    }

    static class Names extends LinkedHashMap<Name, Schema> {
        private String space;                         // default namespace

        public Names() {
        }

        public Names(String space) {
            this.space = space;
        }

        public String space() {
            return space;
        }

        public void space(String space) {
            this.space = space;
        }

        @Override
        public Schema get(Object o) {
            Name name;
            if (o instanceof String) {
                Type primitive = PRIMITIVES.get((String) o);
                if (primitive != null) return Schema.create(primitive);
                name = new Name((String) o, space);
                if (!containsKey(name))                   // if not in default
                    name = new Name((String) o, "");         // try anonymous
            } else {
                name = (Name) o;
            }
            return super.get(name);
        }

        public boolean contains(Schema schema) {
            return get(((NamedSchema) schema).name) != null;
        }

        public void add(Schema schema) {
            put(((NamedSchema) schema).name, schema);
        }

        @Override
        public Schema put(Name name, Schema schema) {
            if (containsKey(name))
                throw new SchemaParseException("Can't redefine: " + name);
            return super.put(name, schema);
        }
    }

    private static ThreadLocal<Boolean> validateNames
            = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return true;
        }
    };

    private static String validateName(String name) {
        if (!validateNames.get()) return name;        // not validating names
        int length = name.length();
        if (length == 0)
            throw new SchemaParseException("Empty name");
        char first = name.charAt(0);
        if (!(Character.isLetter(first) || first == '_'))
            throw new SchemaParseException("Illegal initial character: " + name);
        for (int i = 1; i < length; i++) {
            char c = name.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_'))
                throw new SchemaParseException("Illegal character in: " + name);
        }
        return name;
    }

    private static final ThreadLocal<Boolean> VALIDATE_DEFAULTS
            = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private static Object validateDefault(String fieldName, Schema schema,
                                          Object defaultValue) {
        if ((defaultValue != null)
                && !isValidDefault(schema, defaultValue)) { // invalid default
            String message = "Invalid default for field " + fieldName
                    + ": " + defaultValue + " not a " + schema;
            if (VALIDATE_DEFAULTS.get())
                throw new DataTypeException(message);     // throw exception
            System.err.println("[WARNING] Avro: " + message); // or log warning
        }
        return defaultValue;
    }

    private static boolean isValidDefault(Schema schema, Object defaultValue) {
        switch (schema.getType()) {
            case STRING:
            case ENUM:
                return (defaultValue instanceof String);
            case BYTES:
            case INT:
                return (defaultValue instanceof Integer);
            case LONG:
                return (defaultValue instanceof Long);
            case FLOAT:
                return (defaultValue instanceof Float);
            case DOUBLE:
                return (defaultValue instanceof Double);
            case BOOLEAN:
                return (defaultValue instanceof Boolean);
            case NULL:
                return defaultValue == null;
            case ARRAY:
                if (!(defaultValue instanceof Collection))
                    return false;
                for (Object element : (Collection<Object>) defaultValue)
                    if (!isValidDefault(schema.getElementType(), element))
                        return false;
                return true;
            case MAP:
                if (!(defaultValue instanceof Map))
                    return false;
                for (Object value : ((Map<Object, Object>) defaultValue).values())
                    if (!isValidDefault(schema.getValueType(), value))
                        return false;
                return true;
            case UNION:                                   // union default: first branch
                return isValidDefault(schema.getTypes().get(0), defaultValue);
            default:
                return false;
        }
    }

    /**
     * No change is permitted on LockableArrayList once lock() has been
     * called on it.
     * @param <E>
     */
  
  /*
   * This class keeps a boolean variable <tt>locked</tt> which is set
   * to <tt>true</tt> in the lock() method. It's legal to call
   * lock() any number of times. Any lock() other than the first one
   * is a no-op.
   * 
   * This class throws <tt>IllegalStateException</tt> if a mutating
   * operation is performed after being locked. Since modifications through
   * iterator also use the list's mutating operations, this effectively
   * blocks all modifications.
   */
    static class LockableArrayList<E> extends ArrayList<E> {
        private static final long serialVersionUID = 1L;
        private boolean locked = false;

        public LockableArrayList() {
        }

        public LockableArrayList(int size) {
            super(size);
        }

        public LockableArrayList(List<E> types) {
            super(types);
        }

        public LockableArrayList(E... types) {
            super(types.length);
            Collections.addAll(this, types);
        }

        public List<E> lock() {
            locked = true;
            return this;
        }

        private void ensureUnlocked() {
            if (locked) {
                throw new IllegalStateException();
            }
        }

        public boolean add(E e) {
            ensureUnlocked();
            return super.add(e);
        }

        public boolean remove(Object o) {
            ensureUnlocked();
            return super.remove(o);
        }

        public E remove(int index) {
            ensureUnlocked();
            return super.remove(index);
        }

        public boolean addAll(Collection<? extends E> c) {
            ensureUnlocked();
            return super.addAll(c);
        }

        public boolean addAll(int index, Collection<? extends E> c) {
            ensureUnlocked();
            return super.addAll(index, c);
        }

        public boolean removeAll(Collection<?> c) {
            ensureUnlocked();
            return super.removeAll(c);
        }

        public boolean retainAll(Collection<?> c) {
            ensureUnlocked();
            return super.retainAll(c);
        }

        public void clear() {
            ensureUnlocked();
            super.clear();
        }

    }

}
