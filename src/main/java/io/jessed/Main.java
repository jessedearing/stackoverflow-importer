package io.jessed;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

public class Main {

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder().addContactPoints("cstar1.jessed.io", "cstar2.jessed.io", "cstar3.jessed.io", "cstar4.jessed.io").build();
        Session session = cluster.connect();
        DumpUserParser dumpUserParser = new DumpUserParser("Users.xml");
        try {
            dumpUserParser.Parse(session);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        finally {
            session.close();
            cluster.close();
        }
    }
}
