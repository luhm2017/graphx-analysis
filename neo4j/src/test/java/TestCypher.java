import org.neo4j.driver.v1.*;

import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by Administrator on 2017/8/2 0002.
 */
public class TestCypher {
    Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "123456"));

    public int addEmployees(final String companyName) {
        try (Session session = driver.session()) {
            int employees = 0;
            List<Record> persons = session.readTransaction(new TransactionWork<List<Record>>() {
                @Override
                public List<Record> execute(Transaction tx) {
                    return matchPersonNodes(tx);
                }
            });
            for (final Record person : persons) {
                employees += session.writeTransaction(new TransactionWork<Integer>() {
                    @Override
                    public Integer execute(Transaction tx) {
                        tx.run("MATCH (emp:Person {name: $person_name}) " +
                                        "MERGE (com:Company {name: $company_name}) " +
                                        "MERGE (emp)-[:WORKS_FOR]->(com)",
                                parameters("person_name", person.get("name").asString(), "company_name",
                                        companyName));
                        return 1;
                    }
                });
            }
            return employees;
        }
    }

    private static List<Record> matchPersonNodes(Transaction tx) {
        return tx.run("MATCH (a:Person) RETURN a.name AS name").list();
    }
}
