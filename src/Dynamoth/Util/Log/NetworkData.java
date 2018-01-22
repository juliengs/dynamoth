package Dynamoth.Util.Log;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

public class NetworkData {

    static Map<String, Long> rxCurrentMap = new HashMap<String, Long>();
    static Map<String, List<Long>> rxChangeMap = new HashMap<String, List<Long>>();
    static Map<String, Long> txCurrentMap = new HashMap<String, Long>();
    static Map<String, List<Long>> txChangeMap = new HashMap<String, List<Long>>();
    private static Sigar sigar;
    private static long lastGetMetric = 0;

    /**
     * @throws InterruptedException
     * @throws SigarException
     * 
     */
    public NetworkData(Sigar s) throws SigarException, InterruptedException {
    	lastGetMetric = System.currentTimeMillis();
        sigar = s;
        getMetric();
        System.out.println(networkInfo());
        Thread.sleep(1000);     
    }

    public synchronized static String networkInfo() throws SigarException {
        String info = sigar.getNetInfo().toString();
        info += "\n"+ sigar.getNetInterfaceConfig().toString();
        return info;
    }

    public synchronized static String getDefaultGateway() throws SigarException {
        return sigar.getNetInfo().getDefaultGateway();
    }

    public synchronized static void startMetricTest() throws SigarException, InterruptedException {
        while (true) {
            Long[] m = getMetric();
            long totalrx = m[0];
            long totaltx = m[1];
            
            System.out.print("totalrx(download): ");
            System.out.println("\t" + Sigar.formatSize(totalrx));
            System.out.print("totaltx(upload): ");
            System.out.println("\t" + Sigar.formatSize(totaltx));
            System.out.println("-----------------------------------");
            
            Thread.sleep(1000);
        }

    }

    public synchronized static Long[] getMetric() throws SigarException {
    	long timeDelta = System.currentTimeMillis() - lastGetMetric;
    	lastGetMetric = System.currentTimeMillis();
        for (String ni : sigar.getNetInterfaceList()) {
            // System.out.println(ni);
            NetInterfaceStat netStat = sigar.getNetInterfaceStat(ni);
            NetInterfaceConfig ifConfig = sigar.getNetInterfaceConfig(ni);
            String hwaddr = null;
            if (!NetFlags.NULL_HWADDR.equals(ifConfig.getHwaddr())) {
                hwaddr = ifConfig.getHwaddr();
            }
            if (hwaddr != null) {
                long rxCurrenttmp = netStat.getRxBytes();
                saveChange(rxCurrentMap, rxChangeMap, hwaddr, rxCurrenttmp, ni);
                long txCurrenttmp = netStat.getTxBytes();
                saveChange(txCurrentMap, txChangeMap, hwaddr, txCurrenttmp, ni);
            }
        }
        long totalrxDown = getMetricData(rxChangeMap);
        long totaltxUp = getMetricData(txChangeMap);
        for (List<Long> l : rxChangeMap.values())
            l.clear();
        for (List<Long> l : txChangeMap.values())
            l.clear();
        // Adjuts using timeDelta...
        // Divide by (timeDelta / 1000.0) => if timedelta = 1000ms, then no adjustment is needed
        //totalrxDown = Math.round(totalrxDown / (timeDelta / 1000.0));
        //totaltxUp = Math.round(totaltxUp / (timeDelta / 1000.0));
        // Not needed yet.
        
        return new Long[] { totalrxDown, totaltxUp };
    }

    private synchronized static long getMetricData(Map<String, List<Long>> rxChangeMap) {
        long total = 0;
        for (Entry<String, List<Long>> entry : rxChangeMap.entrySet()) {
            int average = 0;
            for (Long l : entry.getValue()) {
                average += l;
            }
            if (entry.getValue().size() != 0)
            	total += average / entry.getValue().size();
        }
        return total;
    }

    private synchronized static void saveChange(Map<String, Long> currentMap,
            Map<String, List<Long>> changeMap, String hwaddr, long current,
            String ni) {
        Long oldCurrent = currentMap.get(ni);
        if (oldCurrent != null) {
            List<Long> list = changeMap.get(hwaddr);
            if (list == null) {
                list = new LinkedList<Long>();
                changeMap.put(hwaddr, list);
            }
            list.add((current - oldCurrent));
        }
        currentMap.put(ni, current);
    }

}
