/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.container.entrypoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.artifact.ArtifactFetchManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import java.io.File;
import java.util.Collection;

/** An {@link ApplicationClusterEntryPoint} which is started with a job in a predefined location. */
@Internal
public final class StandaloneApplicationClusterEntryPoint extends ApplicationClusterEntryPoint {

    private StandaloneApplicationClusterEntryPoint(
            final Configuration configuration, final PackagedProgram program) {
        super(configuration, program, StandaloneResourceManagerFactory.getInstance());
    }

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, StandaloneApplicationClusterEntryPoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final StandaloneApplicationClusterConfiguration clusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new StandaloneApplicationClusterConfigurationParserFactory(),
                        StandaloneApplicationClusterEntryPoint.class);

        Configuration configuration = loadConfigurationFromClusterConfig(clusterConfiguration);
        if (clusterConfiguration
                .getSavepointRestoreSettings()
                .getRestoreMode()
                .equals(RestoreMode.LEGACY)) {
            LOG.warn(
                    "The {} restore mode is deprecated, please use {} or {} mode instead.",
                    RestoreMode.LEGACY,
                    RestoreMode.CLAIM,
                    RestoreMode.NO_CLAIM);
        }
        PackagedProgram program = null;
        try {
            PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            LOG.info(
                    "Install default filesystem for fetching user artifacts in Standalone Application Mode.");
            FileSystem.initialize(configuration, pluginManager);
            SecurityContext securityContext = installSecurityContext(configuration);
            program =
                    securityContext.runSecured(
                            () -> getPackagedProgram(clusterConfiguration, configuration));
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            configureExecution(configuration, program);
        } catch (Exception e) {
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }

        StandaloneApplicationClusterEntryPoint entrypoint =
                new StandaloneApplicationClusterEntryPoint(configuration, program);

        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }

    @Override
    protected boolean supportsReactiveMode() {
        return true;
    }

    @VisibleForTesting
    static Configuration loadConfigurationFromClusterConfig(
            StandaloneApplicationClusterConfiguration clusterConfiguration) {
        Configuration configuration = loadConfiguration(clusterConfiguration);
        setStaticJobId(clusterConfiguration, configuration);
        SavepointRestoreSettings.toConfiguration(
                clusterConfiguration.getSavepointRestoreSettings(), configuration);
        return configuration;
    }

    private static PackagedProgram getPackagedProgram(
            final StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration flinkConfiguration)
            throws Exception {
        final File userLibDir = ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null);

        File jobJar = null;
        Collection<File> artifacts = null;
        if (clusterConfiguration.hasJars()) {
            ArtifactFetchManager fetchMgr = new ArtifactFetchManager(flinkConfiguration);
            ArtifactFetchManager.Result res =
                    fetchMgr.fetchArtifacts(clusterConfiguration.getJars());

            jobJar = res.getJobJar();
            artifacts = res.getArtifacts();
        }

        final PackagedProgramRetriever programRetriever =
                DefaultPackagedProgramRetriever.create(
                        userLibDir,
                        jobJar,
                        artifacts,
                        clusterConfiguration.getJobClassName(),
                        clusterConfiguration.getArgs(),
                        flinkConfiguration);
        return programRetriever.getPackagedProgram();
    }

    private static void setStaticJobId(
            StandaloneApplicationClusterConfiguration clusterConfiguration,
            Configuration configuration) {
        final JobID jobId = clusterConfiguration.getJobId();
        if (jobId != null) {
            configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        }
    }
}
