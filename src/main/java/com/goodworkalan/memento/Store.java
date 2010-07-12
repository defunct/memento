package com.goodworkalan.memento;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.goodworkalan.stringbeans.json.JsonParser;


/**
 * Client interface to the distributed storage.
 *
 * @author Alan Gutierrez
 */
public class Store {
    /** Direct serializer, probably not the way it will really be. */
    private Serializer serializer;

    /** The concurrent map of owner instance references. */
    private final ConcurrentMap<WeakIdentityReference, UUID> uuids = new ConcurrentHashMap<WeakIdentityReference, UUID>();
    
    /**
     * Create a store with a serializer that it sends messages directly to.
     * Probably not the way it will work. Probably will just take a connection
     * URI.
     * 
     * @param serializer
     *            The serializer.
     */
    public Store(Serializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Refresh the object and return the object version.
     * 
     * @param object
     *            The object.
     * @return The version.
     */
    public long refresh(Object object) {
        ByteBuffer bytes = serializer.read(getIdentifier(object));
        Charset charset = Charset.forName("UTF-8");
        Map<String, Object> map = new JsonParser(charset.decode(bytes)).object();
        return (Long) map.get("version");
    }

    /**
     * Determine if the object is managed by this store.
     * 
     * @param object
     *            The object.
     * @return True if the object is managed by this store.
     */
    public boolean isManaged(Object object) {
        return uuids.containsKey(new WeakIdentityReference(object, null));
    }

    /**
     * Get the UUID of the given object.
     * 
     * @param object
     *            The object.
     * @return The UUID.
     * @exception IllegalArgumentException
     *                If the object is not managed.
     */
    public UUID getIdentifier(Object object) {
        UUID uuid = uuids.get(new WeakIdentityReference(object, null));
        if (uuid == null) {
            
        }
        return uuid;
    }

    /**
     * Commit the writes in the given block of code to the distributed storage,
     * retrying if the distributed storage engine detects an unrepeatable read.
     * 
     * @param commitment
     *            The block of persistence code.
     */
    public void commit(Mutation commitment) {
        for (;;) {
            try {
                commitment.mutate(new Mutator(this));
                break;
            } catch (NonRepatableReadException e) {
                continue;
            }
        }
    }

    /**
     * Format the exception message using the message arguments to format the
     * message found with the message key in the message bundle found in the
     * package of the given context class.
     * 
     * @param contextClass
     *            The context class.
     * @param code
     *            The error code.
     * @param arguments
     *            The format message arguments.
     * @return The formatted message.
     */
    final static String _(Class<?> contextClass, String code, Object...arguments) {
        String baseName = contextClass.getPackage().getName() + ".exceptions";
        String messageKey = contextClass.getSimpleName() + "/" + code;
        try {
            ResourceBundle bundle = ResourceBundle.getBundle(baseName, Locale.getDefault(), Thread.currentThread().getContextClassLoader());
            return String.format((String) bundle.getObject(messageKey), arguments);
        } catch (Exception e) {
            return String.format("Cannot load message key [%s] from bundle [%s] becuase [%s].", messageKey, baseName, e.getMessage());
        }
    }
}
