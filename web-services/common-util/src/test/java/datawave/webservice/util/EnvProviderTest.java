package datawave.webservice.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnvProviderTest {
    
    @Test
    public void testNormalValue() {
        String normalProperty = "ChangeMe";
        String resolved = EnvProvider.resolve(normalProperty);
        assertEquals(normalProperty, resolved);
    }
    
    @Test
    public void testTargetNotFound() {
        String target = "env:" + RandomStringUtils.randomAlphanumeric(25);
        String resolved = EnvProvider.resolve(target);
        assertEquals(target, resolved);
    }
}