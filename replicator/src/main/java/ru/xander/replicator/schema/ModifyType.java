package ru.xander.replicator.schema;

public enum ModifyType {
    DATATYPE,
    DEFAULT,
    MANDATORY,
    NONE;

    public boolean anyOf(ModifyType... modifyTypes) {
        for (ModifyType modifyType : modifyTypes) {
            if (this == modifyType) {
                return true;
            }
        }
        return false;
    }
}
