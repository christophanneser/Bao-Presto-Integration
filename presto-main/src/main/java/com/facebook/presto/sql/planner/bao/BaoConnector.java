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
import com.facebook.presto.sql.planner.optimizations.EffectiveOptimizerPart;
import org.json.simple.JSONArray;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getBaoDriverSocket;

/// The BaoConnector talks to the benchmark driver; it sends query plans, query spans, and execution stats to the driver;
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
        int maxRetries = 30;
        for (int i = 0; i < maxRetries; i++) {
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

    // Send execution stats to Bao
    public void exportExecutionTimeInfo(String json)
    {
        sendData("json:" + json);
    }

    // Send graphviz query plan to Bao
    public void exportGraphivzPlan(String graphviz)
    {
        sendData("dot:" + graphviz);
    }

    // Send json query plan to Bao
    public void exportJsonPlan(String json)
    {
        sendData("json:" + json);
    }

    // Send effective optimizers to Bao
    public void exportEffectiveOptimizerParts(Collection<EffectiveOptimizerPart> effectiveOptimizerParts)
    {
        exportOptimizers("effective", effectiveOptimizerParts);
    }

    // Send required optimizers to Bao
    public void exportRequiredOptimizerParts(Collection<EffectiveOptimizerPart> effectiveOptimizerParts)
    {
        exportOptimizers("required", effectiveOptimizerParts);
    }

    private void exportOptimizers(String type, Collection<EffectiveOptimizerPart> effectiveOptimizers)
    {
        sendData("json:span:" + type + ":" + JSONArray.toJSONString(effectiveOptimizers.stream().map(EffectiveOptimizerPart::toJSON).collect(Collectors.toList())));
    }
}
