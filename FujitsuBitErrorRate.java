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

import org.apache.commons.configuration.XMLConfiguration;
import org.onosproject.drivers.odtn.util.NetconfSessionUtility;
import org.onosproject.drivers.utilities.XmlConfigParser;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.behaviour.BitErrorRateState;
import org.onosproject.net.driver.AbstractHandlerBehaviour;
import org.onosproject.netconf.NetconfController;
import org.onosproject.netconf.NetconfException;
import org.onosproject.netconf.NetconfSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class FujitsuBitErrorRate extends AbstractHandlerBehaviour implements BitErrorRateState  {
    private static final Logger log = LoggerFactory.getLogger(FujitsuBitErrorRate.class);

    private static final String RPC_TAG_NETCONF_BASE =
            "<rpc xmlns=\"urn:ietf:params:xml:ns:netconf:base:1.0\">";

    private static final String RPC_CLOSE_TAG = "</rpc>";
    private static final String OPERATION_ENABLE = "ENABLED";
    private static final String PRE_FEC_BER_TAG = "pre-fec-ber";
    /*
     * This method returns the instance of NetconfController from DriverHandler.
     */
    private NetconfController getController() {
        return handler().get(NetconfController.class);
    }

    /**
     * Get the BER value pre FEC.
     *
     * @param deviceId the device identifier
     * @param port     the port identifier
     * @return the decimal value of BER
     */
    @Override
    public Optional<Double> getPreFecBer(DeviceId deviceId, PortNumber port) {
        NetconfSession session = NetconfSessionUtility
                .getNetconfSession(deviceId, getController());
        checkNotNull(session);
        log.info("port number"+port.toString());
        if(!port.equals(PortNumber.portNumber("11001"))){
            return Optional.empty();
        }
            String slotIndex = FujitsuTerminalDeviceDiscovery
                    .mapPortNumberToFujitsuName(port).split("E")[1].concat("0");
        log.info("°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°");
        log.info(slotIndex);
        log.info("°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°");
        if (Integer.valueOf(slotIndex) > 0) {

            log.debug("REQUEST getPreFecBer to device: {}", getOtnState(slotIndex));

            String reply;
            try {
                reply = session.get(getOtnState(slotIndex));
                if(reply==null){
                    return Optional.empty();
                }
            } catch (Exception e) {
                throw new IllegalStateException(new NetconfException("Failed to retrieve getPreFecBer info.", e));
            }

            log.debug("REPLY from device: {}", reply);

            XMLConfiguration xconf = (XMLConfiguration) XmlConfigParser.loadXmlString(reply);
            if (xconf == null) {
                log.error("Error in executing RPC");
                return Optional.empty();
            }

            String preFecBer = xconf.getString(("data.terminal-device.logical-channels.channel." +
                    "otn.state.pre-fec-ber.instant"));

            log.debug("currentPreFecBer from device: {}", preFecBer);
            if (preFecBer == null) {
                return Optional.empty();
            }

            return Optional.of(Double.parseDouble(preFecBer));
        }

        return Optional.empty();
    }

    @Override
    public Optional<Double> getPostFecBer(DeviceId deviceId, PortNumber port) {
        return Optional.empty();
    }

    private String getOtnState(String slotNumber) {
        StringBuilder filter = new StringBuilder();
        filter.append("<terminal-device xmlns=\"http://openconfig.net/yang/terminal-device\">");
        filter.append("<logical-channels>");
        filter.append("<channel>");
        filter.append("<index>" +slotNumber+ "</index>");
        filter.append("<config>");
        filter.append("<admin-state>" + OPERATION_ENABLE  + "</admin-state>");
        filter.append("</config>");
        filter.append("<otn/>");
        filter.append("<state/>");
        filter.append("</channel>");
        filter.append("</logical-channels>");
        filter.append("</terminal-device>");
        return filteredGetBuilder(filter.toString());
    }

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

}
