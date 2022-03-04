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
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.onlab.packet.ChassisId;
import org.onosproject.drivers.utilities.XmlConfigParser;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceDescription;
import org.onosproject.net.device.DefaultDeviceDescription;
import org.onosproject.net.device.DeviceDescriptionDiscovery;
import org.onosproject.net.device.PortDescription;
import org.onosproject.net.driver.AbstractHandlerBehaviour;
import org.onosproject.net.optical.device.OchPortHelper;
import org.onosproject.net.optical.device.OduCltPortHelper;
import org.onosproject.netconf.NetconfController;
import org.onosproject.netconf.NetconfSession;
import org.onosproject.netconf.NetconfDevice;
import org.onosproject.netconf.NetconfException;
import org.onosproject.odtn.behaviour.OdtnDeviceDescriptionDiscovery;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;


/**
 * Driver Implementation of the DeviceDescription discovery for Fujitsu-T600 terminal devices.
 */
public class FujitsuTerminalDeviceDiscovery
    extends AbstractHandlerBehaviour
    implements OdtnDeviceDescriptionDiscovery, DeviceDescriptionDiscovery {

    private static final String RPC_TAG_NETCONF_BASE =
        "<rpc xmlns=\"urn:ietf:params:xml:ns:netconf:base:1.0\">";

    private static final String RPC_CLOSE_TAG = "</rpc>";

    private static final String TYPES_TRANSCEIVER = "TRANSCEIVER";

    private static final String OC_PLATFORM_TYPES_PORT = "oc-platform-types:PORT";

    private static final String OC_PLATFORM_ACTIVE = "oc-platform-types:ACTIVE";

    private static final String COMPONENT_PORT_PREFIX = "port-";

    private static final String COMPONENT_ETH_PREFIX = "eth-";

    private static final String TYPES_OPTICAL_CHANNEL = "OPTICAL_CHANNEL";

    private static final Logger log = getLogger(FujitsuTerminalDeviceDiscovery.class);

    /**
     * Returns the NetconfSession with the device for which the method was called.
     *
     * @param deviceId device identifier
     *
     * @return The netconf session or null
     */
    private NetconfSession getNetconfSession(DeviceId deviceId) {
        NetconfController controller = handler().get(NetconfController.class);
        NetconfDevice ncdev = controller.getDevicesMap().get(deviceId);
        if (ncdev == null) {
            log.trace("No netconf device, returning null session");
            return null;
        }
        return ncdev.getSession();
    }

    /**
     * Get the deviceId for which the methods apply.
     *
     * @return The deviceId as contained in the handler data
     */
    private DeviceId did() {
        return handler().data().deviceId();
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
     * Builds a request to get Device details, operational data.
     *
     * @return A string with the Netconf RPC for a get with subtree rpcing based on
     *    /components/component/state/
     */
    private String getDeviceDetailsBuilder(){
        StringBuilder filter = new StringBuilder();
        filter.append("<components xmlns='http://openconfig.net/yang/platform'>");
        filter.append("<component>");
        filter.append("<state>");
        filter.append("</state>");
        filter.append("</component>");
        filter.append("</components>");

        return filteredGetBuilder(filter.toString());
    }

    /**
     * Builds a request to get Device Components, config and operational data.
     *
     * @return A string with the Netconf RPC for a get with subtree rpcing based on
     *    /components/
     */
    private String getComponentsRequest() {
        StringBuilder filter = new StringBuilder();
        filter.append("<components xmlns='http://openconfig.net/yang/platform'>");
        filter.append("</components>");
        return filteredGetBuilder(filter.toString());
    }

    /**
     * Builds a request to get Device interfaces, config and operational data.
     *
     * @param portName client port name.
     *
     * @return A string with the Netconf RPC for a get with subtree rpcing based on
     *    /interfaces/interface/name/ethernet/rate being
     */
    private String getInterfacesRequest(String portName) {
        StringBuilder filter = new StringBuilder();
        filter.append("<interfaces xmlns='urn:ietf:params:xml:ns:yang:ietf-interfaces'>");
        filter.append("<interface>");
        filter.append("<name>"+ portName + "</name>");
        filter.append("<ethernet xmlns='urn:fujitsu:params:xml:ns:yang:interface:eth'>");
        filter.append("<rate>");
        filter.append("</rate>");
        filter.append("</ethernet>");
        filter.append("</interface>");
        filter.append("</interfaces>");
        return filteredGetBuilder(filter.toString());
    }

    /**
     * Returns a DeviceDescription with Device info.
     *
     * @return DeviceDescription or null
     */
    @Override
    public DeviceDescription discoverDeviceDetails() {
        boolean defaultAvailable = true;
        SparseAnnotations annotations = DefaultAnnotations.builder().build();

        log.debug("ClientLineTerminalDeviceDiscovery::discoverDeviceDetails device {}", did());

        Device.Type type =
            Device.Type.TERMINAL_DEVICE;

        String vendor       = "NOVENDOR";
        String serialNumber = "0xCAFEBEEF";
        String hwVersion    = "0.2.1";
        String swVersion    = "0.2.1";
        String chassisId    = "128";

        NetconfSession session = getNetconfSession(did());

        try {

            String requestedComponent = session.rpc(getDeviceDetailsBuilder()).get();

            XMLConfiguration xconf = (XMLConfiguration) XmlConfigParser.loadXmlString(requestedComponent);

            vendor       = xconf.getString("data.components.component.state.mfg-name", vendor);
            serialNumber = xconf.getString("data.components.component.state.serial-no", serialNumber);
            swVersion    = xconf.getString("data.components.component.state.software-version", swVersion);
            hwVersion    = xconf.getString("data.components.component.state.hardware-version", hwVersion);
            chassisId    = xconf.getString("data.components.component.state.id", chassisId);

        } catch (Exception e) {
                log.error("ClientLineTerminalDeviceDiscovery::discoverDeviceDetails - Failed to retrieve session {}",
                        did());
                throw new IllegalStateException(new NetconfException("Failed to retrieve version info.", e));
        }

        ChassisId cid = new ChassisId(Long.valueOf(chassisId, 10));

        log.info("Device retrieved details");
        log.info("VENDOR    {}", vendor);
        log.info("HWVERSION {}", hwVersion);
        log.info("SWVERSION {}", swVersion);
        log.info("SERIAL    {}", serialNumber);
        log.info("CHASSISID {}", chassisId);

        return new DefaultDeviceDescription(did().uri(),
                    type, vendor, hwVersion, swVersion, serialNumber,
                    cid, defaultAvailable, annotations);
    }

    /**
     * Returns a list of PortDescriptions for the device.
     *
     * @return a list of descriptions.
     */
    @Override
    public List<PortDescription> discoverPortDetails() {
        try {
            XPathExpressionEngine xpe = new XPathExpressionEngine();
            NetconfSession session = getNetconfSession(did());
            if (session == null) {
                log.error("discoverPortDetails called with null session for {}", did());
                return ImmutableList.of();
            }

            String rpcReply = session.rpc(getComponentsRequest()).get();
            XMLConfiguration xconf = (XMLConfiguration) XmlConfigParser.loadXmlString(rpcReply);
            xconf.setExpressionEngine(xpe);
            log.debug("REPLY {}", rpcReply);
            HierarchicalConfiguration components = xconf.configurationAt("data/components");
            return parsePorts(components);
        } catch (Exception e) {
            log.error("Exception discoverPortDetails() {}", did(), e);
            return ImmutableList.of();
        }
    }


    /**
     * Parses port information from OpenConfig XML configuration.
     *
     * @param components the XML document with components root.
     * @return List of ports
     */
    protected List<PortDescription> parsePorts(HierarchicalConfiguration components) {
        return components.configurationsAt("component").stream()
            .filter(component -> {
                    return !component.getString("name", "unknown").equals("unknown") &&
                    component.getString("state/type", "unknown").equals(OC_PLATFORM_TYPES_PORT) &&
                            component.getString("state/oper-status", "unknown").equals(OC_PLATFORM_ACTIVE);
                    })
            .map(component -> {
                try {
                    // Pass the root document for cross-reference
                    return parsePortComponent(component, components);
                } catch (Exception e) {
                    return null;
                }
            }
            )
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Checks if a given component has a subcomponent of a given type.
     *
     * @param component subtree to parse looking for subcomponents.
     * @param components the full components tree, to cross-ref in
     *  case we need to check (sub)components' types.
     *
     * @return true or false
     */
    private boolean hasSubComponentOfType(
            HierarchicalConfiguration component,
            HierarchicalConfiguration components,
            String type) {
        long count = component.configurationsAt("subcomponents/subcomponent")
            .stream()
            .filter(subcomponent -> {
                        String scName = subcomponent.getString("name");
                        StringBuilder sb = new StringBuilder("component[name='");
                        sb.append(scName);
                        sb.append("']/state/type");
                        String scType = components.getString(sb.toString(), "unknown");
                        return scType.equals(type);
                    })
            .count();
        return (count > 0);
    }

    /**
     * Gets the CltSignalType for a port  client port.
     *
     * @param portName client port name.
     *
     * @return true or false
     *
     */
    private CltSignalType getCltSignalType(String portName) {
        try {
            NetconfSession session = getNetconfSession(did());
            if (session == null) {
                log.error("discoverInterfaces called with null session for {}", did());
                return null;
            }
            String reply = session.rpc(getInterfacesRequest(portName.replace(COMPONENT_PORT_PREFIX,
                    COMPONENT_ETH_PREFIX))).get();
            XMLConfiguration xconf = (XMLConfiguration) XmlConfigParser.loadXmlString(reply);
            switch (xconf.getString("data.interfaces.interface.ethernet.rate")) {
                case "1000000":
                    return CltSignalType.CLT_1GBE;
                case "10000000":
                    return CltSignalType.CLT_10GBE;
                case "40000000":
                    return CltSignalType.CLT_40GBE;
                case "100000000":
                    return CltSignalType.CLT_100GBE;
            }
            return null;
        } catch (Exception e) {
            log.error("Exception discoverPortDetails() {}", did(), e);
            return null;
        }
    }

    /**
     * Checks if a given component has a subcomponent of type OPTICAL_CHANNEL.
     * @param component subtree to parse
     * @param components the full components tree, to cross-ref in
     *  case we need to check transceivers or optical channels.
     *
     * @return true or false
     */
    private boolean hasOpticalChannelSubComponent(
            HierarchicalConfiguration component,
            HierarchicalConfiguration components) {
        return hasSubComponentOfType(component, components, TYPES_OPTICAL_CHANNEL);
    }

    /**
     *  Checks if a given component has a subcomponent of type TRANSCEIVER.
     *
     * @param component subtree to parse
     * @param components the full components tree, to cross-ref in
     *  case we need to check transceivers or optical channels.
     *
     * @return true or false
     */
    private boolean hasTransceiverSubComponent(
            HierarchicalConfiguration component,
            HierarchicalConfiguration components) {
        return hasSubComponentOfType(component, components, TYPES_TRANSCEIVER);
    }

    /**
     * Parses a component XML doc into a PortDescription.
     *
     * @param component subtree to parse. It must be a component ot type PORT.
     * @param components the full components tree, to cross-ref in
     *  case we need to check transceivers or optical channels.
     *
     * @return PortDescription or null if component does not have onos-index
     */
    private PortDescription parsePortComponent(
            HierarchicalConfiguration component,
            HierarchicalConfiguration components) {

        Map<String, String> annotations = new HashMap<>();
        String name = component.getString("name");
        String type = component.getString("state/type");


        log.info("Parsing Component {} type {}", name, type);

        annotations.put(OdtnDeviceDescriptionDiscovery.OC_NAME, name);
        annotations.put(OdtnDeviceDescriptionDiscovery.OC_TYPE, type);
        //annotazioni OCHchannel

        component.configurationsAt("properties/property")
            .forEach(property -> {
                    String pn = property.getString("name");
                    String pv = property.getString("state/value");
                    annotations.put(pn, pv);
                    });

        PortNumber portNum = null;
        if (!annotations.containsKey(PORT_TYPE)) {
            if (hasTransceiverSubComponent(component, components)) {
                portNum = mapFujitsuNameToPortNumber(name);
                annotations.put(PORT_TYPE, OdtnPortType.CLIENT.value());
                String transceiver = component.getString("subcomponents/subcomponent/name");
                annotations.put(OdtnDeviceDescriptionDiscovery.OC_TRANSCEIVER,transceiver);
            } else if (hasOpticalChannelSubComponent(component,components)) {
                portNum = mapFujitsuNameToPortNumber(name);
                annotations.put(PORT_TYPE, OdtnPortType.LINE.value());
                String optCh= component.getString("subcomponents/subcomponent/name");
                annotations.put(OdtnDeviceDescriptionDiscovery.OC_OPTICAL_CHANNEL,optCh);
            }
            log.debug("PORT {} number {}", name, portNum);
        }

        if (annotations.get(PORT_TYPE).equals(OdtnPortType.CLIENT.value())) {
            log.debug("PORT {} number {} added as CLIENT port", name, portNum);
            //CltSignalType cltSignalType = getCltSignalType(name);
            return OduCltPortHelper.oduCltPortDescription(portNum,
                    true,
                    CltSignalType.CLT_10GBE,
                    //cltSignalType,
                    DefaultAnnotations.builder().putAll(annotations).build());
        }
        if (annotations.get(PORT_TYPE).equals(OdtnPortType.LINE.value())) {
            log.debug("PORT {} number {} added as LINE port", name, portNum);
            // TODO: To be configured
            OchSignal signalId = OchSignal.newDwdmSlot(ChannelSpacing.CHL_50GHZ, 1);

            log.info(OchPortHelper.ochPortDescription(
                    portNum, true,
                    OduSignalType.ODU4,
                    true,
                    signalId,
                    DefaultAnnotations.builder().putAll(annotations).build()).toString());

            return OchPortHelper.ochPortDescription(
                    portNum, true,
                    OduSignalType.ODU4,
                    true,
                    signalId,
                    DefaultAnnotations.builder().putAll(annotations).build());
        }
        log.error("PORT {} number {} is of UNKNOWN type", name, portNum);
        return null;
    }

    /**
     * Given a name with the format: port {shelf}/{slot}/{padding}/{port}, where:
     * - shelf: integer [1]
     * - slot: integer [1,2]
     * - padding: integer [0]
     * - port: character representing port type (C=client, E=line) followed by integer [1,12]
     * the function returns a string containing an unambiguous integer to be used as a unique port ID by Onos.
     * E.g.: 1/1/0/E1 -> 11001; 1/2/0/E2 -> 12002; 1/1/0/C1 -> 1101; 1/2/0/C11 -> 1211
     *
     * @param name Value of the leaf <component><name> E.g.: 1/1/0/C1
     * @return a PortNumber from an integer
     */
    protected static PortNumber mapFujitsuNameToPortNumber(String name) {
        String[] canonicalName;

        if(name.contains(COMPONENT_PORT_PREFIX))
            name = name.replace(COMPONENT_PORT_PREFIX, "");
        canonicalName = name.split("/");
        if (canonicalName.length != 4) {
            throw new IllegalArgumentException("Incorrect naming format");
        }
        char portLetter = canonicalName[3].toCharArray()[0];
        StringBuilder mappedName = new StringBuilder();
        switch (portLetter) {
            case 'E':
                mappedName.append(canonicalName[0]).append(canonicalName[1])
                        .append("00").append(canonicalName[3].charAt(1));
                break;
            case 'C':
                mappedName.append(canonicalName[0]).append(canonicalName[1]);
                if (Character.getNumericValue(canonicalName[3].charAt(1)) >= 10){
                    mappedName.append(canonicalName[3].charAt(1));
                } else {
                mappedName.append("0").append(canonicalName[3].charAt(1));
                }
                break;
            default:
                throw new IllegalArgumentException("Port letter must be 'E', 'C' ");
        }
        return PortNumber.portNumber(Long.parseLong(mappedName.toString()));
    }

    /**
     * Given a name with the format: port {shelf}/{slot}/{padding}/{port}, where:
     * - shelf: integer [1]
     * - slot: integer [1,2]
     * - padding: integer [0]
     * - port: character representing port type (C=client, E=line) followed by integer [1,12]
     * the function returns a string containing an unambiguous integer to be used as a unique port ID by Onos.
     * E.g.: 1/1/0/E1 -> 11001; 1/2/0/E2 -> 12002; 1/1/0/C1 -> 1101; 1/2/0/C11 -> 1211
     *
     * @param portNumber from an integer
     * @return the vlue of the leaf <component><name> E.g.: 1/1/0/C1
     */
    protected static String mapPortNumberToFujitsuName(PortNumber portNumber) {
        String number = String.valueOf(portNumber.toLong());
        StringBuilder name = new StringBuilder();
        int shelf = Integer.parseInt(number.substring(0,1));
        int slot = Integer.parseInt(number.substring(1,2));
        name.append(shelf);
        name.append("/");
        name.append(slot);
        name.append("/");
        if (number.length() == 5) {
            name.insert(0,"otsi-");
            name.append("0");
            name.append("/");
            name.append("E").append(number.substring(4,5));
        }
        else if(number.length() == 4) {
            name.insert(0,"transceiver-");
            name.append("0");
            name.append("/");
            name.append("C").append(number.substring(3,4));
        } else {
            throw new IllegalArgumentException("Invalid port index number");
        }

        return name.toString();
    }
}
