/*
 * Copyright 2022-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *  This work was partially supported by EC H2020 project B5G-OPEN(101016663).
 */

package org.onosproject.drivers.odtn;

import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration.XMLConfiguration;
import org.onlab.util.Frequency;
import org.onlab.util.Spectrum;
import org.onosproject.drivers.odtn.impl.DeviceConnectionCache;
import org.onosproject.drivers.odtn.impl.FlowRuleParser;
import org.onosproject.drivers.odtn.openconfig.TerminalDeviceFlowRule;
import org.onosproject.drivers.utilities.XmlConfigParser;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.driver.AbstractHandlerBehaviour;
import org.onosproject.net.flow.*;
import org.onosproject.netconf.DatastoreId;
import org.onosproject.netconf.NetconfController;
import org.onosproject.netconf.NetconfException;
import org.onosproject.netconf.NetconfSession;
import org.onosproject.odtn.behaviour.OdtnDeviceDescriptionDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of FlowRuleProgrammable interface for
 * OpenConfig terminal devices.
 */
public class FujitsuFlowRuleProgrammable
        extends AbstractHandlerBehaviour implements FlowRuleProgrammable {

    private static final Logger log =
            LoggerFactory.getLogger(FujitsuFlowRuleProgrammable.class);

    private static final String RPC_TAG_NETCONF_BASE =
            "<rpc xmlns=\"urn:ietf:params:xml:ns:netconf:base:1.0\">";

    private static final String RPC_CLOSE_TAG = "</rpc>";

    private static final String PREFIX_PORT = "port-";
    private static final String PREFIX_TRANSCEIVER = "transceiver-";
    private static final String DEFAULT_TARGET_POWER = "0.0";;
    private static final String OPERATION_ENABLE = "ENABLED";
    private static final String OC_TYPE_PROT_OTN = "oc-opt-types:PROT_OTN";
    private static final String OC_TYPE_PROT_ETH = "oc-opt-types:PROT_ETHERNET";
    private static final String OC_TYPE_PROT_ODUCN = "oc-opt-types:PROT_ODUCN";
    private static final String OC_TYPE_TRIB_RATE_100G = "oc-opt-types:TRIB_RATE_100G";
    private static final String OC_TYPE_PROT_100GE = "oc-opt-types:PROT_100GE";
    private static final String OC_TYPE_PROT_ODU4 = "oc-opt-types:PROT_ODU4";
    private static final String OPTICAL_CHANNEL = "OPTICAL_CHANNEL";
    private static final String LOGICAL_CHANNEL = "LOGICAL_CHANNEL";
    private static final String FALSE = "false";

    /**
     * Apply the flow entries specified in the collection rules.
     *
     * @param rules A collection of Flow Rules to be applied
     * @return The collection of added Flow Entries
     */
    @Override
    public Collection<FlowRule> applyFlowRules(Collection<FlowRule> rules) {
        NetconfSession session = getNetconfSession();
        if (session == null) {
            openConfigError("null session");
            return ImmutableList.of();
        }
        log.info("dentro apply flow rules");
        // Apply the  rules on the device
        Collection<FlowRule> added = rules.stream()
                .map(r -> new TerminalDeviceFlowRule(r, getLinePorts()))
                .filter(xc -> applyFlowRule(session, xc))
                .collect(Collectors.toList());

        for (FlowRule flowRule : added) {
            log.info("OpenConfig added flowrule {}", flowRule);
            getConnectionCache().add(did(), ((TerminalDeviceFlowRule) flowRule).connectionName(), flowRule);
        }
        //Print out number of rules sent to the device (without receiving errors)
        openConfigLog("applyFlowRules added {}", added.size());
        return added;
    }

    /**
     * Get the flow entries that are present on the device.
     *
     * @return A collection of Flow Entries
     */
    @Override
    public Collection<FlowEntry> getFlowEntries() {
        log.debug("getFlowEntries device {} cache size {}", did(), getConnectionCache().size(did()));
        DeviceConnectionCache cache = getConnectionCache();
        if (cache.get(did()) == null) {
            return ImmutableList.of();
        }
        List<FlowEntry> entries = new ArrayList<>();
        for (FlowRule r : cache.get(did())) {
            entries.add(
                    new DefaultFlowEntry(r, FlowEntry.FlowEntryState.ADDED, 0, 0, 0));
        }

        //Print out number of rules actually found on the device that are also included in the cache
        openConfigLog("getFlowEntries fetched connections {}", entries.size());

        return entries;
    }

    /**
     * Remove the specified flow rules.
     *
     * @param rules A collection of Flow Rules to be removed
     * @return The collection of removed Flow Entries
     */
    @Override
    public Collection<FlowRule> removeFlowRules(Collection<FlowRule> rules) {
        NetconfSession session = getNetconfSession();
        if (session == null) {
            openConfigError("null session");
            return ImmutableList.of();
        }
        List<FlowRule> removed = new ArrayList<>();
        for (FlowRule r : rules) {
            try {
                TerminalDeviceFlowRule termFlowRule = new TerminalDeviceFlowRule(r, getLinePorts());
                removeFlowRule(session, termFlowRule);
                getConnectionCache().remove(did(), termFlowRule.connectionName());
                removed.add(r);
            } catch (Exception e) {
                openConfigError("Error {}", e);
                continue;
            }
        }

        //Print out number of removed rules from the device (without receiving errors)
        openConfigLog("removeFlowRules removed {}", removed.size());

        return removed;
    }

    private DeviceConnectionCache getConnectionCache() {
        return DeviceConnectionCache.init();
    }

    // Context so XPath expressions are aware of XML namespaces
    private static final NamespaceContext NS_CONTEXT = new NamespaceContext() {
        @Override
        public String getNamespaceURI(String prefix) {
            if (prefix.equals("oc-platform-types")) {
                return "http://openconfig.net/yang/platform-types";
            }
            if (prefix.equals("oc-opt-term")) {
                return "http://openconfig.net/yang/terminal-device";
            }
            return null;
        }

        @Override
        public Iterator getPrefixes(String val) {
            return null;
        }

        @Override
        public String getPrefix(String uri) {
            return null;
        }
    };


    /**
     * Helper method to get the device id.
     */
    private DeviceId did() {
        return data().deviceId();
    }

    /**
     * Helper method to log from this class adding DeviceId.
     */
    private void openConfigLog(String format, Object... arguments) {
        log.info("OPENCONFIG {}: " + format, did(), arguments);
    }

    /**
     * Helper method to log an error from this class adding DeviceId.
     */
    private void openConfigError(String format, Object... arguments) {
        log.error("OPENCONFIG {}: " + format, did(), arguments);
    }

    /**
     * Helper method to get the Netconf Session.
     */
    private NetconfSession getNetconfSession() {
        NetconfController controller =
                checkNotNull(handler().get(NetconfController.class));
        return controller.getNetconfDevice(did()).getSession();
    }

    /**
     * Construct a String with a Netconf filtered get RPC Message.
     *
     * @param filter A valid XML tree with the filter to apply in the get
     * @return a String containing the RPC XML Document
     */
    private String filteredGetBuilder(String filter) {
        StringBuilder rpc = new StringBuilder(RPC_TAG_NETCONF_BASE);
        rpc.append("<get>");
        rpc.append("<filter type='subtree'>");
        rpc.append(filter);
        rpc.append("</filter>");
        rpc.append("</get>");
        rpc.append(RPC_CLOSE_TAG);
        return rpc.toString();
    }

    /**
     * Get the index of the logical channel with the corresponding optical-channel.
     *
     * @param session
     * @param otsiPort
     * @return
     */
    private Long getLogicalChannelIndex (NetconfSession session, String otsiPort)
            throws NetconfException {
        try {
            String rpcReply = session.rpc(getLogicalChannelRequest(otsiPort)).get();
            XMLConfiguration xconf = (XMLConfiguration) XmlConfigParser.loadXmlString(rpcReply);
            log.debug("REPLY {}", rpcReply);
            String channel = xconf.getString("data.terminal-device.logical-channels.channel." +
                    "logical-channel-assignments.assignment.state.index");
            if (channel == null) {
                return -1L;
            }
            long index = Long.parseLong(channel);
            log.debug("OpticalChannel {} has index {}", otsiPort, index);
            return index;
        } catch (Exception e) {
            throw new NetconfException("error getting the logical channels");
        }
    }

    private void createLineLogicalChannel(NetconfSession session, String otsiPort)
            throws NetconfException {
        StringBuilder sb = new StringBuilder();
        sb.append("<terminal-device xmlns=\"http://openconfig.net/yang/terminal-device\">");
        sb.append("<logical-channels>");
        sb.append("<channel>");
        sb.append("<index>" + getIndex(otsiPort) +"</index>");
        sb.append("<config>");
        sb.append("<index>" + getIndex(otsiPort) +"</index>");
        sb.append("<admin-state>" + OPERATION_ENABLE  + "</admin-state>");
        sb.append("<trib-protocol xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">" + OC_TYPE_PROT_ODUCN + "</trib-protocol>");
        sb.append("<logical-channel-type xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+ OC_TYPE_PROT_OTN + "</logical-channel-type>");
        sb.append("<test-signal>"+FALSE+"</test-signal>");
        sb.append("</config>");
        sb.append("<otn>");
        sb.append("<config>");
        sb.append("<tti-msg-transmit>hello</tti-msg-transmit>");
        sb.append("<tti-msg-expected>hello</tti-msg-expected>");
        sb.append("</config>");
        sb.append("</otn>");
        sb.append("<logical-channel-assignments>");
        sb.append("<assignment>");
        sb.append("<index>" +  getIndex(otsiPort) +"</index>");
        sb.append("<config>");
        sb.append("<index>" +  getIndex(otsiPort) +"</index>");
        sb.append("<assignment-type>"+ OPTICAL_CHANNEL+"</assignment-type>");
        sb.append("<optical-channel>"+ otsiPort +"</optical-channel>");
        sb.append("</config>");
        sb.append("</assignment>");
        sb.append("</logical-channel-assignments>");
        sb.append("</channel>");
        sb.append("</logical-channels>");
        sb.append("</terminal-device>");

        boolean ok =
                session.editConfig(DatastoreId.CANDIDATE, null, sb.toString());
                session.commit();
        if (!ok) {
            throw new NetconfException("error writing the logical channel");
        }
    }

    /**
     * Construct the filter for a get request to retrieve all Logical Channels
     *
     * This method is used to query the device so we can find the
     * list of current logical channel.
     *
     * It is used to discover the highest index curerntly configured.
     *
     * @return The filt content to send to the device.
     */
    private String getLogicalChannelRequest(String otsiPort) {
        StringBuilder filt = new StringBuilder();
            filt.append("<terminal-device xmlns='http://openconfig.net/yang/terminal-device'>");
            filt.append("<logical-channels>");
            filt.append("<channel>");
            filt.append("<logical-channel-assignments>");
            filt.append("<assignment>");
            filt.append("<state>");
            filt.append("<optical-channel>" + otsiPort + "</optical-channel>");
            filt.append("</state>");
            filt.append("</assignment>");
            filt.append("</logical-channel-assignments>");
            filt.append("</channel>");
            filt.append("</logical-channels>");
            filt.append("</terminal-device>");
        return filteredGetBuilder(filt.toString());
    }

    private void deleteLogicalChannel(NetconfSession session, Long index)
            throws NetconfException {
        StringBuilder sb = new StringBuilder();

        sb.append("<terminal-device xmlns='http://openconfig.net/yang/terminal-device'>");
        sb.append("<logical-channels>");
        sb.append("<channel nc:operation=\"delete\">");
        sb.append("<index>" + index + "</index>");
        sb.append("</channel>");
        sb.append("</logical-channels>");
        sb.append("</terminal-device>");
        log.info(sb.toString());

        boolean ok =
                session.editConfig(DatastoreId.CANDIDATE, null, sb.toString());
                session.commit();
        if (!ok) {
            throw new NetconfException("error writing the logical channel");
        }
    }

    private void setOpticalChannelFrequency(NetconfSession session, String optChannel, Frequency freq)
            throws NetconfException {
        StringBuilder sb = new StringBuilder();

        sb.append("<components xmlns='http://openconfig.net/yang/platform'>");
        sb.append("<component>");
        sb.append("<name>" + optChannel + "</name>");
        sb.append("<config>");
        sb.append("<name>" + optChannel + "</name>");
        sb.append("</config>");
        sb.append("<optical-channel xmlns='http://openconfig.net/yang/terminal-device'>");
        sb.append("<config>");
        sb.append("<frequency>" + (long) freq.asMHz() + "</frequency>");
        sb.append("<target-output-power>" + DEFAULT_TARGET_POWER + "</target-output-power>");
        sb.append("</config>");
        sb.append("</optical-channel>");
        sb.append("</component>");
        sb.append("</components>");

        boolean ok =
                session.editConfig(DatastoreId.CANDIDATE, null, sb.toString());
                session.commit();
        if (!ok) {
            throw new NetconfException("error writing channel frequency");
        }
    }

    private void setLogicalChannelAssignment(NetconfSession session, String operation, String client,
                                             Long index)
            throws NetconfException {
        StringBuilder sb = new StringBuilder();
        sb.append("<terminal-device xmlns=\"http://openconfig.net/yang/terminal-device\">");
        sb.append("<logical-channels>");
        sb.append("<channel>");
        sb.append("<index>"+String.valueOf(index).concat(client.split("C")[1])+"</index>");
        sb.append("<config>");
        sb.append("<index>"+String.valueOf(index).concat(client.split("C")[1])+"</index>");
        sb.append("<admin-state>"+ operation +"</admin-state>");
        sb.append("<rate-class xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_TRIB_RATE_100G+"</rate-class>");
        sb.append("<trib-protocol xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_PROT_ODU4+"</trib-protocol>");
        sb.append("<logical-channel-type xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_PROT_OTN+"</logical-channel-type>");
        sb.append("</config>");
        sb.append("<logical-channel-assignments>");
        sb.append("<assignment>");
        sb.append("<index>"+index+"</index>");
        sb.append("<config>");
        sb.append("<index>"+index+"</index>");
        sb.append("<assignment-type>" + LOGICAL_CHANNEL + "</assignment-type>");
        sb.append("<logical-channel>"+index+"</logical-channel>");
        sb.append("</config>");
        sb.append("</assignment>");
        sb.append("</logical-channel-assignments>");
        sb.append("</channel>");
        sb.append("</logical-channels>");
        sb.append("<logical-channels>");
        sb.append("<channel>");
        sb.append("<index>"+String.valueOf(index).concat("0").concat(client.split("C")[1])+"</index>");
        sb.append("<config>");
        sb.append("<index>"+String.valueOf(index).concat("0").concat(client.split("C")[1])+"</index>");
        sb.append("<admin-state>"+ operation +"</admin-state>");
        sb.append("<rate-class xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_TRIB_RATE_100G+"</rate-class>");
        sb.append("<trib-protocol xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_PROT_100GE+"</trib-protocol>");
        sb.append("<logical-channel-type xmlns:oc-opt-types=\"http://openconfig.net/yang/transport-types\">"+OC_TYPE_PROT_ETH+"</logical-channel-type>");
        sb.append("<loopback-mode>NONE</loopback-mode>");
        sb.append("<test-signal>"+FALSE+"</test-signal>");
        sb.append("</config>");
        sb.append("<ingress>");
        sb.append("<config>");
        sb.append("<transceiver>"+client.replace(PREFIX_PORT,PREFIX_TRANSCEIVER)+"</transceiver>");
        sb.append("</config>");
        sb.append("</ingress>");
        sb.append("<logical-channel-assignments>");
        sb.append("<assignment>");
        sb.append("<index>"+index+"</index>");
        sb.append("<config>");
        sb.append("<index>"+index+"</index>");
        sb.append("<assignment-type>"+LOGICAL_CHANNEL+"</assignment-type>");
        sb.append("<logical-channel>"+String.valueOf(index).concat(client.split("C")[1])+"</logical-channel>");
        sb.append("</config>");
        sb.append("</assignment>");
        sb.append("</logical-channel-assignments>");
        sb.append("</channel>");
        sb.append("</logical-channels>");
        sb.append("</terminal-device>");
        log.info(sb.toString());
        boolean ok =
                session.editConfig(DatastoreId.CANDIDATE, null, sb.toString());
                session.commit();
        if (!ok) {
            throw new NetconfException("error writing logical channel assignment");
        }
    }

    private void deleteLogicalChannelAssignment(NetconfSession session, String client, Long index)
            throws NetconfException {
        StringBuilder sb = new StringBuilder();
        sb.append("<terminal-device xmlns=\"http://openconfig.net/yang/terminal-device\">");
        sb.append("<logical-channels>");
        sb.append("<channel nc:operation=\"delete\">");
        sb.append("<index>"+String.valueOf(index).concat(client.split("C")[1])+"</index>");
        sb.append("</channel>");
        sb.append("<channel nc:operation=\"delete\">");
        sb.append("<index>"+String.valueOf(index).concat("0").concat(client.split("C")[1])+"</index>");
        sb.append("</channel>");
        sb.append("</logical-channels>");
        sb.append("</terminal-device>");
        log.info(sb.toString());
        boolean ok =
                session.editConfig(DatastoreId.CANDIDATE, null, sb.toString());
                session.commit();
        if (!ok) {
            throw new NetconfException("error writing logical channel assignment");
        }
    }

    /**
     * Apply a single flowrule to the device.
     *
     * --- Directionality details:
     * Driver supports ADD (INGRESS) and DROP (EGRESS) rules generated by OpticalCircuit/OpticalConnectivity intents
     * the format of the rules are checked in class TerminalDeviceFlowRule
     *
     * However, the physical transponder is always bidirectional as specified in OpenConfig YANG models
     * therefore ADD and DROP rules are mapped in the same xml that ENABLE (and tune) a transponder port.
     *
     * If the intent is generated as bidirectional both ADD and DROP flowrules are generated for each device, thus
     * the same xml is sent twice to the device.
     *
     * @param session   The Netconf session.
     * @param rule      Flow Rules to be applied.
     * @return true if no Netconf errors are received from the device when xml is sent
     * @throws NetconfException if exchange goes wrong
     */
    protected boolean applyFlowRule(NetconfSession session, TerminalDeviceFlowRule rule) {

        //Configuration of LINE side, used for OpticalConnectivity intents
        //--- configure central frequency
        //--- enable the line port
        if (rule.type == TerminalDeviceFlowRule.Type.LINE_INGRESS ||
                rule.type == TerminalDeviceFlowRule.Type.LINE_EGRESS) {

            FlowRuleParser frp = new FlowRuleParser(rule);
            PortNumber linePort = frp.getPortNumber();

            Frequency centralFrequency = frp.getCentralFrequency();
            String otsiPort = getLinePort(linePort);

            long logicalChannelIndex;

            log.info("Sending LINE FlowRule to device {} LINE port {}, frequency {}",
                    did(), linePort, centralFrequency);

            //STEP 1: FIND logical channel
            try {
                logicalChannelIndex= getLogicalChannelIndex(session, otsiPort);
                if(logicalChannelIndex != -1)
                    log.debug("Logical channel already present, skipping creation phase {}", logicalChannelIndex);
                    /*if(getLogicalChannelIndexState(session,logicalChannelIndex).equals(OPERATION_DISABLE)){
                        createLineLogicalChannel(session, otsiPort);
                    }*/
                else
                    createLineLogicalChannel(session, otsiPort);
            } catch (NetconfException e) {
                log.error("{} Error creating the LINE logical channel for port {}", did(),
                        otsiPort);
            }

            sleepOneSecond();

            //STEP 2: Set central frequency and power
            try {
                setOpticalChannelFrequency(session, otsiPort, centralFrequency);
                log.info("Frequency for port {} set to {} THz", otsiPort, centralFrequency.asTHz());
            } catch (NetconfException e) {
                log.error("Error writing central frequency in the component");
                return false;
            }
        }

        //Configuration of CLIENT side, used for OpticalCircuit intents
        //--- associate the client port to the line port
        //--- enable the client port
        //
        //Assumes only one "assignment" per logical-channel with index 1
        //TODO check the OTN mapping of client ports into the line port frame specified by parameter "<allocation>"
        if (rule.type == TerminalDeviceFlowRule.Type.CLIENT_INGRESS ||
                rule.type == TerminalDeviceFlowRule.Type.CLIENT_EGRESS) {

            String clientPortName;
            String linePortName;
            if (rule.type == TerminalDeviceFlowRule.Type.CLIENT_INGRESS) {
                clientPortName = getClientPort(rule.inPort());
                linePortName = getLinePort(rule.outPort());
            } else {
                clientPortName = getClientPort(rule.outPort());
                linePortName = getLinePort(rule.inPort());
            }
            long logicalChannelIndex;
            log.info("Sending CLIENT FlowRule to device {} CLIENT port: {}, LINE port {}",
                    did(), clientPortName, linePortName);

            //STEP 1: FIND logical channel
            try {
                logicalChannelIndex= getLogicalChannelIndex(session, linePortName);
                if(logicalChannelIndex != -1)
                    log.debug("Logical channel already present, skipping creation phase {}", logicalChannelIndex);
                else
                    createLineLogicalChannel(session, linePortName);
            } catch (NetconfException e) {
                log.error("{} Error creating the LINE logical channel for port {}", did(),
                        linePortName);
            }

            sleepOneSecond();

            //STEP 2: SET logical channel
            try {
                logicalChannelIndex= getLogicalChannelIndex(session, linePortName);
                setLogicalChannelAssignment(session, OPERATION_ENABLE, clientPortName, logicalChannelIndex);
            } catch (NetconfException e) {
                log.error("Error setting the logical channel assignment");
                return false;
            }
        }
        return true;
    }

    protected boolean removeFlowRule(NetconfSession session, TerminalDeviceFlowRule rule)
            throws NetconfException {

        //Configuration of LINE side, used for OpticalConnectivity intents
        //--- configure central frequency to ZERO
        //--- disable the line port
        if (rule.type == TerminalDeviceFlowRule.Type.LINE_INGRESS ||
                rule.type == TerminalDeviceFlowRule.Type.LINE_EGRESS) {

            FlowRuleParser frp = new FlowRuleParser(rule);
            String componentName = getLinePort(frp.getPortNumber());

            log.info("Removing LINE FlowRule device {} line port {}", did(), componentName);
            long logicalChannelIndex;

            try {
                logicalChannelIndex = getLogicalChannelIndex(session, componentName);
                deleteLogicalChannel(session,logicalChannelIndex);
            } catch (NetconfException e) {
                log.error("Error disabling the logical channel line side");
                return false;
            }
        }

        //Configuration of CLIENT side, used for OpticalCircuit intents
        //--- configure central frequency to ZERO
        //--- disable the line port
        if (rule.type == TerminalDeviceFlowRule.Type.CLIENT_INGRESS ||
                rule.type == TerminalDeviceFlowRule.Type.CLIENT_EGRESS) {

            String clientPortName;
            String linePortName;
            if (rule.type == TerminalDeviceFlowRule.Type.CLIENT_INGRESS) {
                clientPortName = getClientPort(rule.inPort());
                linePortName = getLinePort(rule.outPort());
            } else {
                clientPortName = getClientPort(rule.inPort());
                linePortName = getLinePort(rule.outPort());
            }
            long logicalChannelIndex;

            log.debug("Removing CLIENT FlowRule device {} client port: {}, line port {}",
                    did(), clientPortName, linePortName);
            try {
                logicalChannelIndex = getLogicalChannelIndex(session, linePortName);
                deleteLogicalChannelAssignment(session, clientPortName,logicalChannelIndex);
            } catch (NetconfException e) {
                log.error("Error disabling the logical channel assignment");
                return false;
            }
        }

        return true;
    }

    private List<PortNumber> getLinePorts() {
        List<PortNumber> linePorts;

        DeviceService deviceService = this.handler().get(DeviceService.class);
        linePorts = deviceService.getPorts(data().deviceId()).stream()
                .filter(p -> p.annotations().value(OdtnDeviceDescriptionDiscovery.PORT_TYPE)
                        .equals(OdtnDeviceDescriptionDiscovery.OdtnPortType.LINE.value()))
                .map(p -> p.number())
                .collect(Collectors.toList());

        return linePorts;

    }

    private String getLinePort(PortNumber portNum) {
        DeviceService deviceService = this.handler().get(DeviceService.class);
        return deviceService.getPort(did(),portNum)
                .annotations().value(OdtnDeviceDescriptionDiscovery.OC_OPTICAL_CHANNEL);
    }

    private String getClientPort(PortNumber portNum) {
        DeviceService deviceService = this.handler().get(DeviceService.class);
        return deviceService.getPort(did(),portNum)
                .annotations().value(OdtnDeviceDescriptionDiscovery.OC_TRANSCEIVER);
    }

    private String getIndex(String otsiPort) {
        String index;
        switch (otsiPort) {
            case "otsi-1/1/0/E1":
                index = String.valueOf(10);
                break;
            case "otsi-1/1/0/E2":
                index = String.valueOf(20);
                break;
            case "otsi-1/2/0/E1":
                index = String.valueOf(30);
                break;
            case "otsi-1/2/0/E2":
                index = String.valueOf(40);
                break;
            default:
                throw new IllegalArgumentException("Incorrect Line Port name");
        }
        return index;
    }

    private void sleepOneSecond(){
        try {
            Thread.sleep(1000);
        }catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }
}
