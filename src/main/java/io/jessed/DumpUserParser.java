package io.jessed;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by jdearing on 2/21/16.
 */
public class DumpUserParser {
    private String filename;
    public DumpUserParser(String filename) {
        this.filename = filename;
    }

    public void Parse(Session session) throws SAXException, ParserConfigurationException {
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        SAXParser saxParser = saxFactory.newSAXParser();
        try {
            saxParser.parse(filename, new SAXRowImport(session));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected class SAXRowImport extends DefaultHandler {
        private Session session;
        private PreparedStatement stmt;
        public SAXRowImport(Session session) {
            this.session = session;
            this.stmt = session.prepare("INSERT INTO stackoverflow.users (id, " +
                    "aboutme, age, creationdate, " +
                    "displayname, downvotes, emailhash, " +
                    "lastaccessdate, location, reputation, " +
                    "upvotes, views, websiteurl)" +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if(qName == "row") {
                try {
                    BoundStatement boundStatement = stmt.bind(Long.valueOf(attributes.getValue("Id")),
                            attributes.getValue("AboutMe"),
                            attributes.getValue("Age"),
                            new SimpleDateFormat("yyyy-MM-dd").parse(attributes.getValue("CreationDate")),
                            attributes.getValue("DisplayName"),
                            Integer.valueOf(attributes.getValue("DownVotes")),
                            attributes.getValue("EmailHash"),
                            new SimpleDateFormat("yyyy-MM-dd").parse(attributes.getValue("LastAccessDate")),
                            attributes.getValue("Location"),
                            Integer.valueOf(attributes.getValue("Reputation")),
                            Integer.valueOf(attributes.getValue("UpVotes")),
                            Integer.valueOf(attributes.getValue("Views")),
                            attributes.getValue("WebsiteUrl")
                    );
                    session.execute(boundStatement);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
                System.out.println("Adding " + attributes.getValue("DisplayName"));
            }
        }
    }
}
