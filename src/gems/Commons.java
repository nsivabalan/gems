package gems;

/* Ayasdi Inc. Copyright 2013 - all rights reserved. */


/**
 * User: david
 * Date: 8/2/13
 * Time: 3:21 PM
 * Description:
 *      Utilities to help the agent and collector communicate with each other.
 */
public class Commons {

    public static String LOCAL_HOST = "127.0.0.1";
    public static int DEFAULT_PORT = 4446;
    public static long POLL_TIME_MS = 1000;
    public static long PUSH_TIME_MS = 1000;
    public static long GOSSIP_TH = 10;
    public static long GOSSIP_INTERVAL_MS = 1000;

    public static enum Protocol{
        TCP("tcp"),
        UDP("udp");
        private final String name;
        Protocol(String name) {
            this.name = name;
        }
        public String getName(){
            return name;
        }
    }


    public static String makeFullAddr(Protocol protocol, String host, int port) {
        return protocol.getName() + "://" + host + ":" + port;
    }
}