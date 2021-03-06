package distSysLab2.comm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.yaml.snakeyaml.Yaml;

import distSysLab2.model.GroupBean;
import distSysLab2.model.NodeBean;
import distSysLab2.model.RuleBean;
import distSysLab2.model.RuleBean.RuleAction;

public class ConfigParser {
    public static int NUM_NODE;
    public static String configurationFile;

    /**
     * Read clock type part in config file.
     */
    public static String readClock() throws UnknownHostException {
        String clockType = null;
        Map<String, ArrayList<Map<String, Object>>> obj = init();

        ArrayList<Map<String, Object>> entry = obj.get("clockType");
        for(Map<String, Object> type : entry) {
            for(Entry<String, Object> item : type.entrySet()) {
                if(item.getKey().equalsIgnoreCase("Type")) {
                    clockType = item.getValue().toString();
                }
            }
        }
        return clockType;
    }

    /**
     * Read configuration part in config file.
     */
    public static HashMap<String, NodeBean> readConfig() throws UnknownHostException {
        HashMap<String, NodeBean> nodeList = new HashMap<String, NodeBean>();
        Map<String, ArrayList<Map<String, Object>>> obj = init();

        ArrayList<Map<String, Object>> entrys = obj.get("configuration");

        for(Map<String, Object> node : entrys) {
            NodeBean bean = new NodeBean();
            for(Entry<String, Object> entry : node.entrySet()) {
                if(entry.getKey().equalsIgnoreCase("Name")) {
                    bean.setName(entry.getValue().toString());
                }
                if(entry.getKey().equalsIgnoreCase("IP")) {
                    bean.setIp(entry.getValue().toString());
                }
                if(entry.getKey().equalsIgnoreCase("Port")) {
                    bean.setPort(Integer.parseInt((entry.getValue().toString())));
                }
            }
            nodeList.put(bean.getName(), bean);
        }
        nodeList.remove(null);
        return nodeList;
    }

    /**
     * Read group definition part in config file.
     */
    @SuppressWarnings("unchecked")
    public static HashMap<String, GroupBean> readGroup() throws UnknownHostException {
        HashMap<String, GroupBean> groupList = new HashMap<String, GroupBean>();
        Map<String, ArrayList<Map<String, Object>>> obj = init();

        ArrayList<Map<String, Object>> entrys = obj.get("groups");

        for(Map<String, Object> node : entrys) {
            GroupBean bean = new GroupBean();
            for(Entry<String, Object> entry : node.entrySet()) {
                if(entry.getKey().equalsIgnoreCase("Name")) {
                    bean.setName(entry.getValue().toString());
                }
                if(entry.getKey().equalsIgnoreCase("Members")) {
                    for(String member : (ArrayList<String>) entry.getValue()) {
                        bean.addMember(member);
                    }
                }
            }
            groupList.put(bean.getName(), bean);
        }
        groupList.remove(null);
        return groupList;
    }

    /**
     * Read send rule part in config file.
     */
    public static ArrayList<RuleBean> readSendRules() {
        ArrayList<RuleBean> sendRules = new ArrayList<RuleBean>();
        Map<String, ArrayList<Map<String, Object>>> obj = init();

        ArrayList<Map<String, Object>> entrys = obj.get("sendRules");

        for(Map<String, Object> rule : entrys) {
            RuleBean bean = new RuleBean();
            for(Map.Entry<String, Object> item : rule.entrySet()) {
                if(item.getKey().equalsIgnoreCase("Action")) {
                    if(item.getValue().toString().equalsIgnoreCase("Drop")) {
                        bean.setAction(RuleAction.DROP);
                    }
                    if(item.getValue().toString().equalsIgnoreCase("Delay")) {
                        bean.setAction(RuleAction.DELAY);
                    }
                    if(item.getValue().toString().equalsIgnoreCase("Duplicate")) {
                        bean.setAction(RuleAction.DUPLICATE);
                    }
                }

                if(item.getKey().equalsIgnoreCase("Src")) {
                    bean.setSrc(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("Dest")) {
                    bean.setDest(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("Kind")) {
                    bean.setKind(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("seqNum")) {
                    bean.setSeqNum(Integer.valueOf(item.getValue().toString()));
                }
            }
            sendRules.add(bean);
        }
        return sendRules;
    }

    /**
     * Read receive rule part in config file.
     */
    public static ArrayList<RuleBean> readRecvRules() {
        ArrayList<RuleBean> recvRules = new ArrayList<RuleBean>();
        Map<String, ArrayList<Map<String, Object>>> obj = init();

        ArrayList<Map<String, Object>> entrys = obj.get("receiveRules");

        for(Map<String, Object> rule : entrys) {
            RuleBean bean = new RuleBean();
            for(Map.Entry<String, Object> item : rule.entrySet()) {
                if(item.getKey().equalsIgnoreCase("Action")) {
                    if(item.getValue().toString().equalsIgnoreCase("Drop")) {
                        bean.setAction(RuleAction.DROP);
                    }
                    if(item.getValue().toString().equalsIgnoreCase("Delay")) {
                        bean.setAction(RuleAction.DELAY);
                    }
                    if(item.getValue().toString().equalsIgnoreCase("Duplicate")) {
                        bean.setAction(RuleAction.DUPLICATE);
                    }
                }

                if(item.getKey().equalsIgnoreCase("Src")) {
                    bean.setSrc(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("Dest")) {
                    bean.setDest(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("Kind")) {
                    bean.setKind(item.getValue().toString());
                }
                if(item.getKey().equalsIgnoreCase("seqNum")) {
                    bean.setSeqNum(Integer.valueOf(item.getValue().toString()));
                }
                if(item.getKey().equalsIgnoreCase("Duplicate")) {
                    if((item.getValue()).toString().equalsIgnoreCase("True")) {
                        bean.setDuplicate(true);
                    }
                    else if((item.getValue()).toString().equalsIgnoreCase("False")) {
                        bean.setDuplicate(false);
                    }
                }
            }
            recvRules.add(bean);
        }
        return recvRules;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ArrayList<Map<String, Object>>> init() {
        Map<String, ArrayList<Map<String, Object>>> raw = null;
        try {
            // Reads the file and builds configure map
            Yaml yaml = new Yaml();
            File optionFile = new File(configurationFile);
            FileReader fr = new FileReader(optionFile);
            BufferedReader br = new BufferedReader(fr);
            raw = (Map<String, ArrayList<Map<String, Object>>>) yaml.load(br);
        }
        catch (Exception e) {
            System.out.println("File Read Error: " + e.getMessage());
        }
        return raw;
    }

    /**
     * Generate checksum array of a input file.
     */
    private static byte[] createChecksum(String filename) {
        InputStream fis;
        MessageDigest complete = null;

        try {
            fis = new FileInputStream(filename);

            byte[] buffer = new byte[1024];
            complete = MessageDigest.getInstance("MD5");
            int numRead;

            do {
                numRead = fis.read(buffer);
                if(numRead > 0) {
                    complete.update(buffer, 0, numRead);
                }
            }
            while(numRead != -1);

            fis.close();
        }
        catch (IOException e) {
            // Auto-generated catch block
            e.printStackTrace();
        }
        catch (NoSuchAlgorithmException e){
        	// Auto-generated catch block
            e.printStackTrace();
        }
        return complete.digest();
    }

    /**
     * Generate MD5 value of a input file.
     *
     * @param filename
     *            The input file.
     * @return The MD5 value of input file.
     */
    public synchronized static String getMD5Checksum(String filename) {
        byte[] b;
        String result = "";
        b = createChecksum(filename);

        for(int i = 0; i < b.length; i++) {
            result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
        }

        return result;
    }
}
