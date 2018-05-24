#
# Fluentd Kubernetes Metadata Filter Plugin - Enrich Fluentd events with
# Kubernetes metadata
#
# Copyright 2017 Red Hat, Inc.
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
require_relative 'kubernetes_metadata_common'

module KubernetesMetadata
  module WatchNamespaces

    include ::KubernetesMetadata::Common

    def start_namespace_watch
      begin
        if @specific_pod
          watcher          = @client.watch_namespaces({:name => @namespace_name})
        else
          resource_version = @client.get_namespaces.resourceVersion
          watcher          = @client.watch_namespaces(resource_version)
        end
      rescue Exception=>e
        message = "start_namespace_watch: Exception encountered setting up namespace watch from Kubernetes API #{@apiVersion} endpoint #{@kubernetes_url}: #{e.message}"
        message += " (#{e.response})" if e.respond_to?(:response)
        log.debug(message)
        raise Fluent::ConfigError, message
      end
      watcher.each do |notice|
        case notice.type
          when 'MODIFIED'
            namespace_id = notice.object['metadata']['uid']
            namespace_cached    = @namespace_cache[namespace_id]
            if namespace_cached
              @namespace_cache[namespace_id] = parse_namespace_metadata(notice.object)
              @stats.bump(:namespace_cache_watch_updates)
            else
              @stats.bump(:namespace_cache_watch_misses)
            end

            if @specific_pod
              id_cache_key = get_id_cache_key_of_specific_pod
              id_cached = @id_cache[id_cache_key]
              if id_cached && id_cached[:namespace_id] != namespace_id
                id_cached[:namespace_id] = namespace_id
                @stats.bump(:id_cache_watch_updates_namespace);
              end
            end
          when 'DELETED'
            # ignore and let age out for cases where 
            # deleted but still processing logs
            @stats.bump(:namespace_cache_watch_deletes_ignored)
          else
            # Don't pay attention to creations, since the created namespace may not
            # be used by any pod on this node.
            @stats.bump(:namespace_cache_watch_ignored)
        end
      end
    end

  end
end
