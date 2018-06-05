#
# Fluentd Kubernetes Metadata Filter Plugin - Enrich Fluentd events with
# Kubernetes metadata
#
# Copyright 2015 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require_relative '../helper'
require 'ostruct'
require_relative 'watch_test'

class WatchNamespaceTestTest < WatchTest

     include KubernetesMetadata::WatchNamespace

     setup do
       @added = OpenStruct.new(
         type: 'ADDED',
         object: {
           'metadata' => {
                'name' => 'added',
                'uid' => 'added_uid'
            }
         }
       )
       @modified = OpenStruct.new(
         type: 'MODIFIED',
         object: {
           'metadata' => {
                'name' => 'foo',
                'uid' => 'modified_uid'
            }
         }
       )
       @deleted = OpenStruct.new(
         type: 'DELETED',
         object: {
           'metadata' => {
                'name' => 'deleteme',
                'uid' => 'deleted_uid'
            }
         }
       )
     end

    test 'namespace watch updates cache when MODIFIED is received' do
      @namespace_cache['original_uid'] = {}
      @client.stub :watch_namespaces, [@modified] do
       start_namespace_watch
       assert_equal(true, @namespace_cache.key?('original_uid'))
       assert_equal(true, @namespace_cache.key?('modified_uid'))
       assert_equal(1, @stats[:namespace_cache_watch_updates])
      end
    end

    test 'namespace watch updates cache when ADDED is received' do
      @client.stub :watch_namespaces, [@added] do
       start_namespace_watch
       assert_equal(true, @namespace_cache.key?('added_uid'))
       assert_equal(1, @stats[:namespace_cache_watch_updates])
      end
    end

    test 'namespace watch ignore MODIFIED on id cache if info is not cached' do
      @client.stub :watch_namespaces, [@modified] do
       start_namespace_watch
       assert_equal(true, @namespace_cache.key?('modified_uid'))
       assert_equal(1, @stats[:namespace_cache_watch_updates])
      end
    end

    test 'namespace watch updates id cache when MODIFIED is received and namespace uid is changed' do
      cache_key = get_id_cache_key_given_metadata_source
      @id_cache[cache_key] = { :namespace_id => 'original_uid' }
      @client.stub :watch_namespaces, [@modified] do
       start_namespace_watch
       assert_equal('modified_uid', @id_cache[cache_key][:namespace_id])
       assert_equal(1, @stats[:id_cache_watch_updates_namespace])
      end
    end

    test 'namespace watch ignores DELETED' do
      @namespace_cache['deleted_uid'] = {}
      @client.stub :watch_namespaces, [@deleted] do
       start_namespace_watch
       assert_equal(true, @namespace_cache.key?('deleted_uid'))
       assert_equal(1, @stats[:namespace_cache_watch_ignored])
      end
    end

end
