package org.yamcs.http.api;

import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;
import org.yamcs.ConfigurationException;
import org.yamcs.YConfiguration;
import org.yamcs.http.api.NameDescriptionSearchMatcher;
import org.yamcs.mdb.XtceDbFactory;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.XtceDb;

public class NameDescriptionSearchMatcherTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        YConfiguration.setupTest("refmdb");
        XtceDbFactory.reset();
    }

    @Test
    public void testSearchMatch() throws ConfigurationException {
        XtceDb mdb = XtceDbFactory.createInstanceByConfig("refmdb");
        assertTrue(match("/REFMDB/CcSdS-APID", mdb));
        assertTrue(match("REFMDB_ccsds-apid", mdb));
        assertTrue(match("ap ReFmDB_CC", mdb));
    }

    private boolean match(String searchTerm, XtceDb mdb) {
        NameDescriptionSearchMatcher matcher = new NameDescriptionSearchMatcher(searchTerm);
        for (Parameter p : mdb.getParameters()) {
            if (matcher.matches(p)) {
                return true;
            }
        }
        return false;
    }
}
