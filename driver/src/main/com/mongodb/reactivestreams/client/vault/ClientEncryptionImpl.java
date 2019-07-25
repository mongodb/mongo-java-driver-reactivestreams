/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.vault;

import com.mongodb.Block;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.reactivestreams.client.internal.ObservableToPublisher;
import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.reactivestreams.Publisher;

import static com.mongodb.assertions.Assertions.notNull;

@SuppressWarnings("deprecation")
class ClientEncryptionImpl implements ClientEncryption {
    private final com.mongodb.async.client.vault.ClientEncryption wrapped;

    public ClientEncryptionImpl(final com.mongodb.async.client.vault.ClientEncryption wrapped) {
        this.wrapped = notNull("wrapped", wrapped);
    }

    @Override
    public Publisher<BsonBinary> createDataKey(final String kmsProvider) {
        return createDataKey(kmsProvider, new DataKeyOptions());
    }

    @Override
    public Publisher<BsonBinary> createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        return new ObservableToPublisher<BsonBinary>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<BsonBinary>>(){
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<BsonBinary> callback) {
                        wrapped.createDataKey(kmsProvider, dataKeyOptions, callback);
                    }
                }));
    }

    @Override
    public Publisher<BsonBinary> encrypt(final BsonValue value, final EncryptOptions options) {
        return new ObservableToPublisher<BsonBinary>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<BsonBinary>>(){
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<BsonBinary> callback) {
                        wrapped.encrypt(value, options, callback);
                    }
                }));
    }

    @Override
    public Publisher<BsonValue> decrypt(final BsonBinary value) {
        return new ObservableToPublisher<BsonValue>(com.mongodb.async.client.Observables.observe(
                new Block<com.mongodb.async.SingleResultCallback<BsonValue>>(){
                    @Override
                    public void apply(final com.mongodb.async.SingleResultCallback<BsonValue> callback) {
                        wrapped.decrypt(value, callback);
                    }
                }));
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
