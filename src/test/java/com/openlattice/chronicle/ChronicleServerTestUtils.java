package com.openlattice.chronicle;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author alfoncenzioka &lt;alfonce@openlattice.com&gt;
 */
public class ChronicleServerTestUtils {

    public static String getRandomElement( List<String> userTypes ) {
        Random random = new Random(  );
        return userTypes.get( random.nextInt(userTypes.size()) );
    }

    public static Set<Object> getRandomElements( List<String> userTypes ) {
        Set<Object> result = new HashSet<>(  );
        Random random = new Random(  );
        int numItems = random.nextInt(userTypes.size() + 1);


        for (int i = 0; i < numItems; i++) {
            result.add( getRandomElement( userTypes ) );
        }
        return result;
    }
}
