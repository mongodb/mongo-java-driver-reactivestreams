/*
 * Copyright 2014-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.ConnectionString;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.client.MongoDriverInformation;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.security.CodeSource;
import java.util.jar.Attributes;
import java.util.jar.Manifest;


/**
 * A factory for MongoClient instances.
 *
 */
public final class MongoClients {

    /**
     * Creates a new client with the default connection string "mongodb://localhost".
     *
     * @return the client
     */
    public static MongoClient create() {
        return create(new ConnectionString("mongodb://localhost"));
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection
     * @return the client
     */
    public static MongoClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(connectionString, null);
    }


    /**
     * Create a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static MongoClient create(final ConnectionString connectionString, final MongoDriverInformation mongoDriverInformation) {
        return new MongoClientImpl(com.mongodb.async.client.MongoClients.create(connectionString,
                getMongoDriverInformation(mongoDriverInformation)));
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static MongoClient create(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation) {
        return new MongoClientImpl(com.mongodb.async.client.MongoClients.create(settings,
                getMongoDriverInformation(mongoDriverInformation)));
    }

    private static MongoDriverInformation getMongoDriverInformation(final MongoDriverInformation mongoDriverInformation) {
        if (mongoDriverInformation == null) {
            return DEFAULT_DRIVER_INFORMATION;
        } else {
            return MongoDriverInformation.builder(mongoDriverInformation)
                    .driverName(DRIVER_NAME)
                    .driverVersion(DRIVER_VERSION).build();
        }
    }

    private static final String DRIVER_NAME = "mongo-java-driver-reactivestreams";
    private static final String DRIVER_VERSION = getDriverVersion();
    private static final MongoDriverInformation DEFAULT_DRIVER_INFORMATION = MongoDriverInformation.builder().driverName(DRIVER_NAME)
            .driverVersion(DRIVER_VERSION).build();

    private static String getDriverVersion() {
        String driverVersion = "unknown";

        try {
            CodeSource codeSource = MongoClients.class.getProtectionDomain().getCodeSource();
            if (codeSource != null) {
                String path = codeSource.getLocation().getPath();
                URL jarUrl = path.endsWith(".jar") ? new URL("jar:file:" + path + "!/") : null;
                if (jarUrl != null) {
                    JarURLConnection jarURLConnection = (JarURLConnection) jarUrl.openConnection();
                    Manifest manifest = jarURLConnection.getManifest();
                    String version = (String) manifest.getMainAttributes().get(new Attributes.Name("Build-Version"));
                    if (version != null) {
                        driverVersion = version;
                    }
                }
            }
        } catch (SecurityException e) {
            // do nothing
        } catch (IOException e) {
            // do nothing
        }
        return driverVersion;
    }

    private MongoClients() {
    }
}
