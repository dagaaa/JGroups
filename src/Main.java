import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {


        System.setProperty("java.net.preferIPv4Stack", "true");
//        JGroups jGroups=new JGroups();
        new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.1"));
//        jGroups.receive();
        DistributedMap distributedMap= new DistributedMap();
        Scanner scanner= new Scanner(System.in);
        String msg;
        while (true){
            msg=scanner.nextLine();
            String[] tmp =msg.split(" ");
            if (tmp[0].equals("put")){
                distributedMap.put(tmp[1],tmp[2]);
            }
            else if(tmp[0].equals("remove")){
                distributedMap.remove(tmp[1]);
            }
            else if(tmp[0].equals("containsKey")){
                System.out.println(distributedMap.containsKey(tmp[1]));
            }
            else if(tmp[0].equals("get")){
                System.out.println(distributedMap.get(tmp[1]));
            }


        }



    }

}
