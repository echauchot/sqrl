package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.name.Name;

import java.util.List;

public class FlexibleSchema {



    public static class Element {

        Name name;
        SchemaElementDescription description;
        Object default_value;

    }

    public static class Table extends Element {

        TableFieldType typeDef;

    }

    public static class Field extends Element {

        List<FieldType> types;

    }

    public static abstract class FieldType {

        Name variantName;

        boolean isArray;
        boolean nonNull;

        List<Constraint> constraints;

    }

    public static class TableFieldType extends FieldType {

        List<Field> fields;

    }

    public class BasicFieldType extends FieldType {

        ScalarType datatype;

    }


}
