package com.lakala.datacenter.enums;


/**
 * Created by Administrator on 2017/7/11 0011.
 */
public enum GraphEnum {
    TERMINAL("terminal", RelationshipTypes.terminal), BANKCARD("bankcard", RelationshipTypes.bankcard);
    private String relType;
    private RelationshipTypes relationshipTypes;


    private GraphEnum(String relType, RelationshipTypes relationshipTypes) {
        this.relType = relType;
        this.relationshipTypes = relationshipTypes;
    }

    public String getRelType() {
        return relType;
    }

    public RelationshipTypes getRelationshipTypes(String relType) {
        for (GraphEnum ge : GraphEnum.values()) {
            if (ge.relType.equals(relType)) return ge.relationshipTypes;
            continue;
        }
        return null;
    }

    public RelationshipTypes getRelationshipTypes() {
        return relationshipTypes;
    }

}
