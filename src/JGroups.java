import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JGroups {
    private JChannel channel = new JChannel(false);
    private ProtocolStack stack = new ProtocolStack();

    public JGroups(ConcurrentHashMap<String, String> concurrentMap) {

        channel.setProtocolStack(stack);

        stack.addProtocol(new UDP())
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE());

        try {
            stack.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        receive(concurrentMap);

        try {
            channel.connect("operation");
            channel.getState(null, 0);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(String line) throws Exception {
        Message msg = new Message(null, null, line);
        channel.send(msg);
    }

    public void receive(ConcurrentHashMap<String, String> concurrentMap) {
        channel.setReceiver(new ReceiverAdapter() {
            @Override
            public void viewAccepted(View new_view) {
                handleView(channel, new_view);
            }

            @Override
            public void receive(Message msg) {
//                if (!msg.getSrc().equals(channel.getAddress())) {
                String[] parameters = msg.getObject().toString().split(" ");
                if (parameters[0].equals("put")) {
                    concurrentMap.put(parameters[1], parameters[2]);
                    System.out.println("received msg from "
                            + msg.getSrc() + ": "
                            + msg.getObject());
//                    }
                } else if (parameters[0].equals("remove")) {
                    concurrentMap.remove(parameters[1]);
                    System.out.println("received msg from "
                            + msg.getSrc() + ": "
                            + msg.getObject());
                }
            }

            @Override
            public void getState(OutputStream output) throws Exception {
                synchronized (concurrentMap) {
                    Util.objectToStream(concurrentMap, new DataOutputStream(output));
                }
            }

            @Override
            public void setState(InputStream input) throws Exception {
                ConcurrentHashMap<String, String> cmap;
                cmap = (ConcurrentHashMap<String, String>) Util.objectFromStream(new DataInputStream(input));
                synchronized (concurrentMap) {
                    concurrentMap.clear();
                    concurrentMap.putAll(cmap);
                }
                System.out.println(cmap.size() + " messages in chat history):");
                for (Map.Entry str : cmap.entrySet())
                    System.out.println(str);
            }

            private  void handleView(JChannel ch, View new_view) {
                System.out.println(new_view.toString());
                if (new_view instanceof MergeView) {
                    ViewHandler handler = new ViewHandler(ch, (MergeView) new_view);
                    // requires separate thread as we don't want to block JGroups
                    handler.start();
                }
            }

             class ViewHandler extends Thread {
                JChannel ch;
                MergeView view;

                private ViewHandler(JChannel ch, MergeView view) {
                    this.ch = ch;
                    this.view = view;
                }

                public void run() {
                    List<View> subgroups = view.getSubgroups();
                    View tmp_view = subgroups.get(0); // picks the first
                    Address local_addr = ch.getAddress();
                    if (!tmp_view.getMembers().contains(local_addr)) {
                        System.out.println("Not member of the new primary partition ("
                                + tmp_view + "), will re-acquire the state");
                        try {
                            ch.getState(null, 30000);
                        } catch (Exception ex) {
                        }
                    } else {
                        System.out.println("Not member of the new primary partition ("
                                + tmp_view + "), will do nothing");
                    }
                }
            }

        });
    }


    }
