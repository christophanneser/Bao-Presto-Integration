/*
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
 */
package com.facebook.presto.sql.planner.bao;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import org.json.simple.JSONArray;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.getBaoDriverSocket;

/// The BaoConnector talks to the benchmark driver
public class BaoConnector
{
    private final Logger log;
    private final Session session;

    public BaoConnector(Session session)
    {
        this.session = session;
        log = Logger.get(BaoConnector.class);
    }

    private static String parseHost(String socket)
    {
        String[] result = socket.split(":");
        assert (result.length == 2);
        return result[0];
    }

    private static int parsePort(String socket)
    {
        String[] result = socket.split(":");
        assert (result.length == 2);
        return Integer.parseInt(result[1]);
    }


    private void sendData(String data)
    {
        String connection = getBaoDriverSocket(session);
        String host = parseHost(connection);
        int port = parsePort(connection);

        for (int i = 0; i < 30; i++) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);

                //todo refactor server settings to session properties?

                Socket socket = new Socket(host, port);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                int size = data.length();
                byte[] message = data.getBytes(StandardCharsets.UTF_8);

                out.writeInt(size);
                out.write(message);

                out.close();
                socket.close();
                return;
            }
            catch (IOException | InterruptedException ignored) {
            }
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            catch (InterruptedException ignored) {
            }
        }
        log.error("Could not notify the benchmark driver!");
    }

    //******************************************************************************
    // Send execution stats to driver server
    //******************************************************************************
    public void exportExecutionTimeInfo(String json)
    {
        sendData("json:" + json);
    }

    public void exportGraphivzPlan(String graphviz)
    {
        sendData("dot:" + graphviz);
    }

    public void exportJsonPlan(String json)
    {
        sendData("json:" + json);
    }

    //******************************************************************************
    // Export optimizers steps (1-81)
    //******************************************************************************
    public void exportEffectiveOptimizers(List<String> effectiveOptimizers)
    {
        sendData("json:optimizers:effective:" + JSONArray.toJSONString(effectiveOptimizers));
    }

    public void exportRequiredOptimizers(List<String> requiredOptimizers)
    {
        sendData("json:optimizers:required:" + JSONArray.toJSONString(requiredOptimizers));
    }

    //******************************************************************************
    // Export rules (1-100)
    //******************************************************************************
    public void exportEffectiveRules(List<String> effectiveRules)
    {
        sendData("json:rules:effective:" + JSONArray.toJSONString(effectiveRules));
    }

    public void exportRequiredRules(List<String> requiredRules)
    {
        sendData("json:rules:required:" + JSONArray.toJSONString(requiredRules));
    }

    //private boolean sendData(byte[] data)
    //{
    //    try {
    //        Socket socket = new Socket("localhost", 9381);
    //        OutputStream outputStream = socket.getOutputStream();
    //        outputStream.write(data);
    //    }
    //    catch (UnknownHostException e) {
    //        log.error("Cannot connect to BAO server");
    //    }
    //    catch (IOException ioException) {
    //        return false;
    //    }
    //    return true;
    //}

    //public void sendPlan(String json)
    //{
    //    if (sendData(json.getBytes(StandardCharsets.UTF_8))) {
    //        log.info("Sent json plan to BAO server");
    //    }
    //}
}
