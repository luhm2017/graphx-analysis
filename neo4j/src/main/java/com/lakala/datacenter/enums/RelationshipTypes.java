package com.lakala.datacenter.enums;

import org.neo4j.graphdb.RelationshipType;

/**
 * Created by Administrator on 2017/5/31 0031.
 */
public enum RelationshipTypes implements RelationshipType {
    terminal, bankcard, loginmobile, ipv4, applymymobile, hometel, recommend, identification, email, company, companyaddress, companytel, emergencymobile,merchantmobile,channelmobile,relativemobile, relativecontact, device;
}
