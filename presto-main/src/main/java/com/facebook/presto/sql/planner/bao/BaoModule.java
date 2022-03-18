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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class BaoModule
        implements Module
{
    public BaoModule()
    {
    }

    public void configure(Binder binder)
    {
        binder.bind(BaoConnector.class).in(Scopes.SINGLETON);
        // ConfigBinder.configBinder(binder).bindConfig(NodeConfig.class);
        // ExportBinder.newExporter(binder).export(NodeInfo.class).withGeneratedName();
    }
}
