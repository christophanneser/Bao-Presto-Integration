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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.server.PrestoServer;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaoCLIParser
{
    private BaoCLIParser() {}

    private static boolean toggleOptimization(String query, Logger log, Session session)
    {
        Pattern pattern = Pattern.compile("(disable)\\s+([a-zA-Z,]*)\\s*(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(query);
        if (matcher.matches()) {
            String setting = matcher.group(2);
            String[] values = matcher.group(3).split(",");

            switch (setting.toLowerCase(Locale.getDefault())) {
                case "optimizers":
                    session.getOptimizerConfiguration().disableOptimizers(Arrays.asList(values));
                    log.info("Disable optimizers:" + Arrays.toString(values));
                    return true;
                case "rules":
                    session.getOptimizerConfiguration().disableRules(Arrays.asList(values));
                    log.info("Disable rules:" + Arrays.toString(values));
                    return true;
            }
        }
        if (query.equalsIgnoreCase("reset optimizers")) {
            session.getOptimizerConfiguration().reset();
            log.info("Successfully reset optimizer configuration!");
            return true;
        }
        return false;
    }

    // parse custom CLI settings
    public static boolean parseCLI(String query, Session session)
    {
        Logger log = Logger.get(PrestoServer.class);
        return toggleOptimization(query, log, session);
    }
}
